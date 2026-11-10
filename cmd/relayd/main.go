package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"relayd"
)

func main() {
	var cfg relayd.Config
	var peers, listen, socket string
	flag.StringVar(&cfg.Node, "node", "", "unique node name")
	flag.StringVar(&cfg.StateDir, "state-dir", "./relayd-state", "private persistent directory")
	flag.StringVar(&cfg.FluxSocket, "fluxd-socket", "/run/fluxd/fluxd.sock", "actual local Fluxd socket")
	flag.StringVar(&cfg.OrbitalURL, "orbitald", "http://127.0.0.1:8080", "local orbitald URL")
	flag.StringVar(&peers, "peers", "", "comma-separated peer HTTP URLs")
	flag.StringVar(&listen, "listen", "127.0.0.1:8090", "peer API address")
	flag.StringVar(&socket, "socket", "./relayd.sock", "private input socket for orbitald")
	flag.Int64Var(&cfg.MaxBytes, "max-bytes", 1<<30, "maximum encoded bytes per acquisition and task")
	flag.Parse()
	if peers != "" {
		for _, p := range strings.Split(peers, ",") {
			cfg.Peers = append(cfg.Peers, strings.TrimSpace(p))
		}
	}
	a, err := relayd.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	// Never unlink an existing socket: it could belong to a live daemon.
	l, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := a.ServeFlux(l); err != nil && !strings.Contains(err.Error(), "closed network connection") {
			log.Print(err)
		}
	}()
	s := &http.Server{Addr: listen, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second}
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		<-signals
		s.Close()
		l.Close()
	}()
	log.Printf("relayd node=%s listen=%s input-socket=%s", cfg.Node, listen, socket)
	if err = s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
