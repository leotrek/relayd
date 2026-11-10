package relayd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// Record preserves Fluxd metadata without coupling the two Go modules.
type Record struct {
	ID       string `json:"acquisition_id"`
	Status   string `json:"status"`
	Checksum string `json:"checksum"`
	Bytes    int64  `json:"compressed_bytes"`
}
type frame struct {
	Error   string            `json:"error,omitempty"`
	Record  json.RawMessage   `json:"record,omitempty"`
	Records []json.RawMessage `json:"records,omitempty"`
	Status  string            `json:"status,omitempty"`
	Bytes   int64             `json:"bytes,omitempty"`
}
type request struct {
	Op string `json:"op"`
	ID string `json:"id,omitempty"`
}

func readFrame(r io.Reader, v any) error {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return err
	}
	if size == 0 || size > 4<<20 {
		return fmt.Errorf("invalid frame size")
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > 4<<20 {
		return fmt.Errorf("frame too large")
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}
func (a *Agent) flux(ctx context.Context, op, id string) (frame, net.Conn, error) {
	var f frame
	c, err := a.dialFlux(ctx, "unix", a.cfg.FluxSocket)
	if err != nil {
		return f, nil, err
	}
	deadline := time.Now().Add(30 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	c.SetDeadline(deadline)
	if err = writeFrame(c, request{op, id}); err == nil {
		err = readFrame(c, &f)
	}
	if err == nil && f.Error != "" {
		err = fmt.Errorf("fluxd: %s", f.Error)
	}
	if err != nil {
		c.Close()
		return f, nil, err
	}
	return f, c, nil
}
func validateRecord(raw json.RawMessage, id string, max int64) (Record, error) {
	var r Record
	err := json.Unmarshal(raw, &r)
	if err != nil {
		return r, err
	}
	digest, e := hex.DecodeString(r.Checksum)
	if r.ID != id || r.Status != "READY" || r.Bytes < 0 || r.Bytes > max || e != nil || len(digest) != sha256.Size {
		return r, fmt.Errorf("invalid or oversized acquisition %s", id)
	}
	return r, nil
}
func (a *Agent) cached(id string) (json.RawMessage, error) {
	b, err := os.ReadFile(filepath.Join(a.cfg.StateDir, "cache", id, "metadata.json"))
	return b, err
}
func (a *Agent) local(ctx context.Context, id string) (json.RawMessage, error) {
	if !safeID.MatchString(id) {
		return nil, fmt.Errorf("invalid acquisition ID")
	}
	if b, err := a.cached(id); err == nil {
		return b, nil
	}
	f, c, err := a.flux(ctx, "GetAcquisition", id)
	if err != nil {
		return nil, err
	}
	c.Close()
	_, err = validateRecord(f.Record, id, a.cfg.MaxBytes)
	return f.Record, err
}

// acquire copies a complete encoded acquisition to an atomic, verified local cache.
// The caller holds the task mutex; HTTP readers see only published directories.
func (a *Agent) acquire(ctx context.Context, id string) error {
	if _, err := a.local(ctx, id); err == nil {
		return nil
	}
	for _, peer := range a.cfg.Peers {
		req, err := http.NewRequestWithContext(ctx, "GET", peer+"/v1/data/"+id, nil)
		if err != nil {
			return err
		}
		resp, err := a.client.Do(req)
		if err != nil {
			continue
		}
		err = a.receive(id, resp)
		resp.Body.Close()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("no reachable verified replica for %s", id)
}
func (a *Agent) receive(id string, resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned %s", resp.Status)
	}
	var f frame
	if err := readFrame(resp.Body, &f); err != nil {
		return err
	}
	r, err := validateRecord(f.Record, id, a.cfg.MaxBytes)
	if err != nil {
		return err
	}
	if f.Error != "" || f.Bytes != r.Bytes {
		return fmt.Errorf("invalid transfer descriptor")
	}
	root := filepath.Join(a.cfg.StateDir, "cache")
	temp, err := os.MkdirTemp(root, ".incoming-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	out, err := os.Create(filepath.Join(temp, "payload"))
	if err != nil {
		return err
	}
	h := sha256.New()
	_, err = io.CopyN(io.MultiWriter(out, h), resp.Body, r.Bytes)
	if err == nil {
		var b [1]byte
		n, e := resp.Body.Read(b[:])
		if n != 0 || e != io.EOF {
			err = fmt.Errorf("trailing transfer bytes")
		}
	}
	if err == nil && hex.EncodeToString(h.Sum(nil)) != r.Checksum {
		err = fmt.Errorf("checksum mismatch")
	}
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	meta, err := os.OpenFile(filepath.Join(temp, "metadata.json"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = meta.Write(f.Record)
	if err == nil {
		err = meta.Sync()
	}
	closeErr = meta.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = syncDir(temp); err != nil {
		return err
	}
	if err = os.Rename(temp, filepath.Join(root, id)); err != nil {
		return err
	}
	return syncDir(root)
}
func (a *Agent) serveData(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	raw, err := a.local(r.Context(), id)
	if err != nil {
		http.Error(w, "acquisition unavailable", 404)
		return
	}
	rec, err := validateRecord(raw, id, a.cfg.MaxBytes)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	var body io.ReadCloser
	if _, err = a.cached(id); err == nil {
		body, err = os.Open(filepath.Join(a.cfg.StateDir, "cache", id, "payload"))
	} else {
		var f frame
		f, body, err = a.flux(r.Context(), "OpenAcquisition", id)
		if err == nil {
			if f.Bytes != rec.Bytes || string(f.Record) != string(raw) {
				err = fmt.Errorf("acquisition changed")
			}
		}
	}
	if body != nil {
		defer body.Close()
	}
	if err != nil {
		http.Error(w, "acquisition unavailable", 502)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if err = writeFrame(w, frame{Record: raw, Bytes: rec.Bytes}); err == nil {
		io.CopyN(w, body, rec.Bytes)
	}
}

// ServeFlux supplies orbitald with local Fluxd records plus transferred cache records.
// The listener must be a private Unix socket; it is owned by the caller.
func (a *Agent) ServeFlux(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go a.serveFlux(c)
	}
}
func (a *Agent) serveFlux(c net.Conn) {
	defer c.Close()
	var opened string
	for {
		c.SetDeadline(time.Now().Add(30 * time.Second))
		var req request
		if err := readFrame(c, &req); err != nil {
			return
		}
		if req.Op == "GetStatus" {
			if writeFrame(c, frame{Status: "ready"}) != nil {
				return
			}
			continue
		}
		if req.Op == "ReleaseAcquisition" {
			f := frame{Status: "released"}
			if opened == "" || opened != req.ID {
				f = frame{Error: "not_open"}
			} else {
				opened = ""
			}
			if writeFrame(c, f) != nil {
				return
			}
			continue
		}
		if opened != "" {
			if writeFrame(c, frame{Error: "release_required"}) != nil {
				return
			}
			continue
		}
		if req.Op != "GetAcquisition" && req.Op != "OpenAcquisition" {
			if writeFrame(c, frame{Error: "unknown_operation"}) != nil {
				return
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		raw, err := a.local(ctx, req.ID)
		if err != nil {
			cancel()
			if writeFrame(c, frame{Error: "not_found"}) != nil {
				return
			}
			continue
		}
		f := frame{Record: raw}
		var body io.ReadCloser
		if req.Op == "OpenAcquisition" {
			var rec Record
			json.Unmarshal(raw, &rec)
			f.Bytes = rec.Bytes
			if _, e := a.cached(req.ID); e == nil {
				body, err = os.Open(filepath.Join(a.cfg.StateDir, "cache", req.ID, "payload"))
			} else {
				f, body, err = a.flux(ctx, req.Op, req.ID)
			}
		}
		if err != nil {
			cancel()
			if writeFrame(c, frame{Error: "not_found"}) != nil {
				return
			}
			continue
		}
		err = writeFrame(c, f)
		if body != nil {
			if err == nil {
				_, err = io.CopyN(c, body, f.Bytes)
			}
			body.Close()
			opened = req.ID
		}
		cancel()
		if err != nil {
			return
		}
	}
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
