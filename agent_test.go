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
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func sample(id string, b []byte) json.RawMessage {
	h := sha256.Sum256(b)
	raw, _ := json.Marshal(map[string]any{"schema_version": 1, "acquisition_id": id, "status": "READY", "compressed_bytes": len(b), "uncompressed_bytes": len(b), "checksum": hex.EncodeToString(h[:]), "raw_checksum": hex.EncodeToString(h[:]), "compression": map[string]string{"codec": "identity", "mode": "lossless"}})
	return raw
}

// In-memory transports keep unit tests independent of host networking privileges.
type memoryServer struct{ URL string }

func (*memoryServer) Close() {}

var handlers = map[string]http.Handler{}
var nextServer int

func memoryHTTP(h http.Handler) *memoryServer {
	nextServer++
	u := fmt.Sprintf("http://peer-%d.test", nextServer)
	handlers[u] = h
	return &memoryServer{URL: u}
}

type memoryTransport struct{}

func (memoryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	h := handlers[r.URL.Scheme+"://"+r.URL.Host]
	if h == nil {
		return nil, fmt.Errorf("unreachable peer")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Result(), nil
}
func mockFlux(a *Agent, data map[string][]byte) {
	a.dialFlux = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			var req request
			if readFrame(server, &req) != nil {
				return
			}
			b, ok := data[req.ID]
			if !ok {
				writeFrame(server, frame{Error: "not_found"})
				return
			}
			f := frame{Record: sample(req.ID, b)}
			if req.Op == "OpenAcquisition" {
				f.Bytes = int64(len(b))
			}
			writeFrame(server, f)
			if req.Op == "OpenAcquisition" {
				server.Write(b)
			}
		}()
		return client, nil
	}
}
func fakeOrbital(t *testing.T) (*memoryServer, *atomic.Int32) {
	t.Helper()
	calls := new(atomic.Int32)
	s := memoryHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			reply(w, 200, map[string]any{"runtime": map[string]string{"error": ""}})
		case "/v1/tasks/window-1":
			reply(w, 200, map[string]string{"status": "stopped", "result_status": "success"})
		case "/v1/tasks":
			var v map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
				t.Error(err)
			}
			if len(v["name"]) == 0 || len(v["inputs"]) == 0 {
				t.Error("missing orbitald fields")
			}
			calls.Add(1)
			reply(w, 202, map[string]any{"window": map[string]string{"id": "window-1"}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	return s, calls
}
func newAgent(t *testing.T, data map[string][]byte, peers ...string) (*Agent, *atomic.Int32) {
	t.Helper()
	orbital, calls := fakeOrbital(t)
	a, err := New(Config{Node: "test", StateDir: t.TempDir(), FluxSocket: "in-memory", OrbitalURL: orbital.URL, Peers: peers})
	if err != nil {
		t.Fatal(err)
	}
	a.client.Transport = memoryTransport{}
	mockFlux(a, data)
	return a, calls
}
func task() Task {
	return Task{ID: "task-1", Name: "inspect", Image: "example:dev", Inputs: []string{"x"}, Deadline: time.Now().Add(time.Minute)}
}
func submit(t *testing.T, a *Agent, v Task) Assignment {
	t.Helper()
	got, err := a.Submit(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func TestLocalExecutionAndDurableDeduplication(t *testing.T) {
	a, calls := newAgent(t, map[string][]byte{"x": []byte("samples")})
	v := task()
	if got := submit(t, a, v); got.State != "accepted" || got.Node != "test" {
		t.Fatal(got)
	}
	// Restart against the same journal and retry concurrently.
	restarted, err := New(a.cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			_, err := restarted.Submit(context.Background(), v)
			if err != nil {
				t.Error(err)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if calls.Load() != 1 {
		t.Fatalf("submitted %d times", calls.Load())
	}
	v.Image = "other:dev"
	if _, err := a.Submit(context.Background(), v); err == nil {
		t.Fatal("accepted conflicting ID")
	}
}
func TestDelegateToPeerHoldingInputs(t *testing.T) {
	b, remoteCalls := newAgent(t, map[string][]byte{"x": []byte("remote")})
	peer := memoryHTTP(b.Handler())
	defer peer.Close()
	a, localCalls := newAgent(t, nil, peer.URL)
	got := submit(t, a, task())
	if got.State != "accepted" || got.Node != peer.URL {
		t.Fatal(got)
	}
	if localCalls.Load() != 0 || remoteCalls.Load() != 1 {
		t.Fatalf("local=%d remote=%d", localCalls.Load(), remoteCalls.Load())
	}
}
func TestJoinTransfersMissingInputAndServesOrbitald(t *testing.T) {
	b, _ := newAgent(t, map[string][]byte{"y": []byte("remote samples")})
	peer := memoryHTTP(b.Handler())
	defer peer.Close()
	a, calls := newAgent(t, map[string][]byte{"x": []byte("local")}, peer.URL)
	v := task()
	v.Inputs = append(v.Inputs, "y")
	if got := submit(t, a, v); got.State != "accepted" || got.Node != "test" {
		t.Fatal(got)
	}
	if calls.Load() != 1 {
		t.Fatal("local task not submitted")
	}
	// Exercise the actual framed socket adapter that orbitald uses.
	client, server := net.Pipe()
	defer client.Close()
	go a.serveFlux(server)
	for _, op := range []string{"GetAcquisition", "OpenAcquisition"} {
		if err := writeFrame(client, request{op, "y"}); err != nil {
			t.Fatal(err)
		}
		var f frame
		if err := readFrame(client, &f); err != nil {
			t.Fatal(err)
		}
		if f.Error != "" {
			t.Fatal(f.Error)
		}
		if op == "OpenAcquisition" {
			body := make([]byte, f.Bytes)
			if _, err := io.ReadFull(client, body); err != nil {
				t.Fatal(err)
			}
			if string(body) != "remote samples" {
				t.Fatal(string(body))
			}
		}
	}
	writeFrame(client, request{"ReleaseAcquisition", "y"})
	var f frame
	if err := readFrame(client, &f); err != nil || f.Status != "released" {
		t.Fatal(f, err)
	}
}
func TestAssignedTaskNeverForwardsAgain(t *testing.T) {
	var probes atomic.Int32
	peer := memoryHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { probes.Add(1); http.Error(w, "no", 404) }))
	defer peer.Close()
	a, calls := newAgent(t, nil, peer.URL)
	v := task()
	v.Assigned = true
	if _, err := a.Submit(context.Background(), v); err == nil {
		t.Fatal("missing input accepted")
	}
	// Data discovery is allowed, but no task/placement request may be forwarded.
	if probes.Load() != 1 || calls.Load() != 0 {
		t.Fatal(probes.Load(), calls.Load())
	}
}
func TestLostAcknowledgementIsNotResubmitted(t *testing.T) {
	a, _ := newAgent(t, map[string][]byte{"x": []byte("x")})
	var calls atomic.Int32
	orbital := memoryHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			reply(w, 200, map[string]any{})
			return
		}
		calls.Add(1)
		// The remote accepted the task, but its acknowledgement was truncated.
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("{"))
	}))
	defer orbital.Close()
	a.cfg.OrbitalURL = orbital.URL
	v := task()
	if got := submit(t, a, v); got.State != "uncertain" {
		t.Fatal(got)
	}
	if got := submit(t, a, v); got.State != "uncertain" {
		t.Fatal(got)
	}
	if calls.Load() != 1 {
		t.Fatal("resubmitted ambiguous task")
	}
}
func TestInterruptedSubmissionSurvivesRestart(t *testing.T) {
	a, calls := newAgent(t, nil)
	v := task()
	if err := a.save(Assignment{Task: v, Node: "peer", State: "submitting"}); err != nil {
		t.Fatal(err)
	}
	if got := submit(t, a, v); got.State != "uncertain" {
		t.Fatal(got)
	}
	if calls.Load() != 0 {
		t.Fatal("replayed interrupted task")
	}
}
func TestInvalidAndExpiredTasks(t *testing.T) {
	a, calls := newAgent(t, nil)
	for _, mutate := range []func(*Task){
		func(v *Task) { v.ID = "../escape" }, func(v *Task) { v.Inputs = []string{"../x"} },
		func(v *Task) { v.Deadline = time.Now().Add(-time.Second) }, func(v *Task) { v.RunTimeout = "-1s" },
		func(v *Task) { v.Image = "" },
	} {
		v := task()
		mutate(&v)
		if _, err := a.Submit(context.Background(), v); err == nil {
			t.Fatal("accepted invalid task", v)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid task submitted")
	}
}
func TestUnreachablePeerDoesNotPreventLocalExecution(t *testing.T) {
	a, calls := newAgent(t, map[string][]byte{"x": []byte("x")}, "http://127.0.0.1:1")
	if got := submit(t, a, task()); got.State != "accepted" || calls.Load() != 1 {
		t.Fatal(got)
	}
}
func TestTransferValidation(t *testing.T) {
	for _, kind := range []string{"corrupt", "truncated", "oversized", "wrong-id", "trailing", "length", "status"} {
		t.Run(kind, func(t *testing.T) {
			a, _ := newAgent(t, nil)
			a.cfg.MaxBytes = 8
			payload := []byte("samples")
			raw := sample("x", payload)
			f := frame{Record: raw, Bytes: int64(len(payload))}
			switch kind {
			case "corrupt":
				payload[0] = 'X'
			case "truncated":
				payload = payload[:2]
			case "oversized":
				f.Record = sample("x", make([]byte, 9))
				f.Bytes = 9
			case "wrong-id":
				f.Record = sample("y", payload)
			case "trailing":
				payload = append(payload, 'x')
			case "length":
				f.Bytes++
			case "status":
				var r map[string]any
				json.Unmarshal(raw, &r)
				r["status"] = "TRANSFERRING"
				f.Record, _ = json.Marshal(r)
			}
			var b bytes.Buffer
			writeFrame(&b, f)
			b.Write(payload)
			resp := &http.Response{StatusCode: 200, Body: io.NopCloser(&b)}
			if err := a.receive("x", resp); err == nil {
				t.Fatal("accepted invalid transfer")
			}
			if _, err := a.cached("x"); !os.IsNotExist(err) {
				t.Fatal("published invalid acquisition")
			}
			entries, _ := os.ReadDir(filepath.Join(a.cfg.StateDir, "cache"))
			if len(entries) != 0 {
				t.Fatal("partial files leaked")
			}
		})
	}
}
func TestFramingRejectsOversizedAndTruncatedFrames(t *testing.T) {
	for _, b := range [][]byte{{0, 0, 0, 0}, {255, 255, 255, 255}, {0, 0, 0, 2, '{'}} {
		var f frame
		if err := readFrame(bytes.NewReader(b), &f); err == nil {
			t.Fatal("accepted invalid frame")
		}
	}
}
func TestHTTPValidation(t *testing.T) {
	a, _ := newAgent(t, nil)
	for _, body := range []string{`{"unknown":1}`, `{} {}`, `{}`} {
		r := httptest.NewRequest("POST", "/v1/ready", bytes.NewBufferString(body))
		w := httptest.NewRecorder()
		a.Handler().ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("got %d want 400: %s", w.Code, w.Body.String())
		}
	}
}
func TestHTTPSubmissionAndStatus(t *testing.T) {
	a, calls := newAgent(t, map[string][]byte{"x": []byte("x")})
	b, _ := json.Marshal(task())
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/tasks", bytes.NewReader(b)))
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	a.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/tasks/task-1", nil))
	if w.Code != 200 || calls.Load() != 1 {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestEncodedTaskByteBudget(t *testing.T) {
	a, calls := newAgent(t, map[string][]byte{"x": []byte("1234"), "y": []byte("5678")})
	a.cfg.MaxBytes = 6
	v := task()
	v.Inputs = append(v.Inputs, "y")
	if _, err := a.Submit(context.Background(), v); err == nil {
		t.Fatal("accepted oversized inputs")
	}
	if calls.Load() != 0 {
		t.Fatal("oversized task submitted")
	}
}
func TestConfigValidation(t *testing.T) {
	for _, u := range []string{"file:///etc/passwd", "http://user:pass@localhost", "http://localhost/path", "http://localhost?query=x"} {
		_, err := New(Config{Node: "a", StateDir: t.TempDir(), FluxSocket: "unused", OrbitalURL: u})
		if err == nil {
			t.Fatal(fmt.Sprintf("accepted %s", u))
		}
	}
}

func TestExecutionStatusFollowsDelegation(t *testing.T) {
	b, _ := newAgent(t, map[string][]byte{"x": []byte("x")})
	peer := memoryHTTP(b.Handler())
	a, _ := newAgent(t, nil, peer.URL)
	submit(t, a, task())
	r := httptest.NewRequest("GET", "/v1/tasks/task-1/execution", nil)
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var state map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil || state["result_status"] != "success" {
		t.Fatal(state, err)
	}
}
