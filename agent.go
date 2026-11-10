// Package relayd coordinates local execution and one-hop peer delegation.
package relayd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Node       string
	FluxSocket string
	OrbitalURL string
	StateDir   string
	Peers      []string
	MaxBytes   int64
}

// Task IDs must be globally unique and submitted through one designated origin.
// Deadline is the latest submission time, not a guarantee of completion.
type Task struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Image      string          `json:"image"`
	Inputs     []string        `json:"inputs"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Deadline   time.Time       `json:"deadline"`
	RunTimeout string          `json:"run_timeout,omitempty"`
	Memory     string          `json:"memory,omitempty"`
	Assigned   bool            `json:"assigned,omitempty"` // Terminal delegation: do not forward again.
}
type Assignment struct {
	Task     Task            `json:"task"`
	Node     string          `json:"node"`
	State    string          `json:"state"` // submitting, accepted, uncertain
	Response json.RawMessage `json:"response,omitempty"`
	Error    string          `json:"error,omitempty"`
}
type Agent struct {
	dialFlux func(context.Context, string, string) (net.Conn, error)
	cfg      Config
	client   *http.Client
	mu       sync.Mutex // Deliberately serializes placement and submission in this prototype.
}

func New(cfg Config) (*Agent, error) {
	if cfg.Node == "" || cfg.StateDir == "" || cfg.FluxSocket == "" {
		return nil, fmt.Errorf("node, state directory and flux socket are required")
	}
	for _, s := range append([]string{cfg.OrbitalURL}, cfg.Peers...) {
		u, err := url.Parse(s)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("invalid service URL %q", s)
		}
	}
	cfg.OrbitalURL = strings.TrimRight(cfg.OrbitalURL, "/")
	cfg.Peers = append([]string(nil), cfg.Peers...)
	for i := range cfg.Peers {
		cfg.Peers[i] = strings.TrimRight(cfg.Peers[i], "/")
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 1 << 30
	}
	if cfg.MaxBytes < 0 {
		return nil, fmt.Errorf("max bytes must be positive")
	}
	for _, dir := range []string{"tasks", "cache"} {
		if err := os.MkdirAll(filepath.Join(cfg.StateDir, dir), 0700); err != nil {
			return nil, err
		}
	}
	return &Agent{cfg: cfg, dialFlux: (&net.Dialer{}).DialContext, client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (a *Agent) call(ctx context.Context, method, endpoint string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned %s", endpoint, resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
}
func (a *Agent) ready(ctx context.Context, t Task) bool {
	var status struct {
		Runtime struct {
			Error string `json:"error"`
		} `json:"runtime"`
	}
	if a.call(ctx, "GET", a.cfg.OrbitalURL+"/v1/status", nil, &status) != nil || status.Runtime.Error != "" {
		return false
	}
	for _, id := range t.Inputs {
		if _, err := a.local(ctx, id); err != nil {
			return false
		}
	}
	return true
}
func (a *Agent) path(id string) string { return filepath.Join(a.cfg.StateDir, "tasks", id+".json") }
func (a *Agent) load(id string) (Assignment, error) {
	var v Assignment
	b, err := os.ReadFile(a.path(id))
	if err == nil {
		err = json.Unmarshal(b, &v)
	}
	return v, err
}
func (a *Agent) save(v Assignment) error {
	f, err := os.CreateTemp(filepath.Join(a.cfg.StateDir, "tasks"), ".task-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	err = json.NewEncoder(f).Encode(v)
	if err == nil {
		err = f.Sync()
	}
	e := f.Close()
	if err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), a.path(v.Task.ID)); err != nil {
		return err
	}
	// Persist the directory entry before making any remote side effect.
	return syncDir(filepath.Dir(a.path(v.Task.ID)))
}
func validate(t Task) error {
	if !safeID.MatchString(t.ID) || t.Name == "" || t.Image == "" {
		return fmt.Errorf("safe task ID, name and image are required")
	}
	if t.Deadline.IsZero() {
		return fmt.Errorf("deadline is required")
	}
	if t.RunTimeout != "" {
		d, err := time.ParseDuration(t.RunTimeout)
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid run_timeout")
		}
	}
	for _, id := range t.Inputs {
		if !safeID.MatchString(id) {
			return fmt.Errorf("invalid acquisition ID")
		}
	}
	if len(t.Payload) > 0 && !json.Valid(t.Payload) {
		return fmt.Errorf("invalid payload")
	}
	return nil
}
func fingerprint(t Task) string {
	t.Assigned = false
	b, _ := json.Marshal(t)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Submit persists the chosen destination before submitting exactly once. A lost
// acknowledgement is uncertain, never permission to execute on another node.
func (a *Agent) Submit(ctx context.Context, t Task) (Assignment, error) {
	if err := validate(t); err != nil {
		return Assignment{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if old, err := a.load(t.ID); err == nil {
		if fingerprint(old.Task) != fingerprint(t) {
			return Assignment{}, fmt.Errorf("task ID reused with a different request")
		}
		if old.State == "submitting" {
			old.State = "uncertain"
			old.Error = "interrupted submission; reconcile with destination"
		}
		return old, nil
	} else if !os.IsNotExist(err) {
		return Assignment{}, err
	}
	if !time.Now().Before(t.Deadline) {
		return Assignment{}, fmt.Errorf("task deadline elapsed")
	}
	ctx, cancel := context.WithDeadline(ctx, t.Deadline)
	defer cancel()
	target := ""
	if !a.ready(ctx, t) && !t.Assigned {
		for _, peer := range a.cfg.Peers {
			var availability struct {
				Ready bool `json:"ready"`
			}
			if a.call(ctx, "POST", peer+"/v1/ready", t, &availability) == nil && availability.Ready {
				target = peer
				break
			}
		}
	}
	if target == "" {
		// If no peer holds all inputs, bring the missing inputs here.
		var total int64
		for _, id := range t.Inputs {
			if err := a.acquire(ctx, id); err != nil {
				return Assignment{}, err
			}
			raw, err := a.local(ctx, id)
			if err != nil {
				return Assignment{}, err
			}
			rec, err := validateRecord(raw, id, a.cfg.MaxBytes)
			if err != nil {
				return Assignment{}, err
			}
			if rec.Bytes > a.cfg.MaxBytes-total {
				return Assignment{}, fmt.Errorf("task inputs exceed byte limit")
			}
			total += rec.Bytes
		}
		var status struct {
			Runtime struct {
				Error string `json:"error"`
			} `json:"runtime"`
		}
		if err := a.call(ctx, "GET", a.cfg.OrbitalURL+"/v1/status", nil, &status); err != nil {
			return Assignment{}, err
		}
		if status.Runtime.Error != "" {
			return Assignment{}, fmt.Errorf("orbitald runtime unavailable: %s", status.Runtime.Error)
		}
	}
	if ctx.Err() != nil {
		return Assignment{}, ctx.Err()
	}
	v := Assignment{Task: t, Node: a.cfg.Node, State: "submitting"}
	if target != "" {
		v.Node = target
	}
	if err := a.save(v); err != nil {
		return Assignment{}, err
	}
	var err error
	if target != "" {
		forwarded := t
		forwarded.Assigned = true
		var remote Assignment
		err = a.call(ctx, "POST", target+"/v1/tasks", forwarded, &remote)
		if err == nil {
			if remote.State != "accepted" {
				err = fmt.Errorf("peer submission %s: %s", remote.State, remote.Error)
			} else {
				v.Response = remote.Response
			}
		}
	} else {
		input := map[string]any{"acquisition_ids": t.Inputs}
		body := map[string]any{"name": t.Name, "image": t.Image, "inputs": input, "payload": t.Payload, "duration": time.Until(t.Deadline).String(), "run_timeout": t.RunTimeout, "memory": t.Memory}
		err = a.call(ctx, "POST", a.cfg.OrbitalURL+"/v1/tasks", body, &v.Response)
	}
	if err != nil {
		v.State = "uncertain"
		v.Error = err.Error()
	} else {
		v.State = "accepted"
	}
	if e := a.save(v); e != nil {
		return Assignment{}, e
	}
	return v, nil
}
func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}
func (a *Agent) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /v1/ready", func(w http.ResponseWriter, r *http.Request) {
		var t Task
		if err := decode(w, r, &t); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := validate(t); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		reply(w, 200, map[string]any{"node": a.cfg.Node, "ready": time.Now().Before(t.Deadline) && a.ready(r.Context(), t)})
	})
	m.HandleFunc("POST /v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		var t Task
		if err := decode(w, r, &t); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		v, err := a.Submit(r.Context(), t)
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		reply(w, 202, v)
	})
	m.HandleFunc("GET /v1/tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !safeID.MatchString(id) {
			http.Error(w, "invalid ID", 400)
			return
		}
		a.mu.Lock()
		v, err := a.load(id)
		a.mu.Unlock()
		if err != nil {
			http.Error(w, "unknown task", 404)
			return
		}
		if v.State == "submitting" {
			v.State = "uncertain"
		}
		reply(w, 200, v)
	})
	m.HandleFunc("GET /v1/tasks/{id}/execution", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !safeID.MatchString(id) {
			http.Error(w, "invalid ID", 400)
			return
		}
		a.mu.Lock()
		v, err := a.load(id)
		a.mu.Unlock()
		if err != nil {
			http.Error(w, "unknown task", 404)
			return
		}
		if v.State != "accepted" {
			http.Error(w, "submission requires reconciliation", 409)
			return
		}
		endpoint := ""
		if v.Node != a.cfg.Node {
			endpoint = v.Node + "/v1/tasks/" + id + "/execution"
		} else {
			var response struct {
				Window struct {
					ID string `json:"id"`
				} `json:"window"`
			}
			if json.Unmarshal(v.Response, &response) != nil || !safeID.MatchString(response.Window.ID) {
				http.Error(w, "invalid orbitald acknowledgement", 502)
				return
			}
			endpoint = a.cfg.OrbitalURL + "/v1/tasks/" + response.Window.ID
		}
		var out json.RawMessage
		if err := a.call(r.Context(), "GET", endpoint, nil, &out); err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		reply(w, 200, out)
	})
	m.HandleFunc("GET /v1/data/{id}", a.serveData)
	return m
}
