package main

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/antongulenko/golib"
	log "github.com/sirupsen/logrus"
)

const TcpNet = "tcp" // Could be explicitly set to tcp4 or tcp6

type TcpBalancer struct {
	frontends map[string]*frontend
}

func NewTcpBalancer(frontends map[string]string, tasks *golib.TaskGroup) *TcpBalancer {
	b := &TcpBalancer{
		frontends: make(map[string]*frontend),
	}
	for name, endpoint := range frontends {
		f := &frontend{
			name:             name,
			backendEndpoints: make(map[string]bool),
		}
		f.task = &golib.TCPListenerTask{
			ListenEndpoint: endpoint,
			Handler:        f.handleConnection,
			StopHook:       f.shutdown,
		}
		b.frontends[name] = f
		tasks.Add(f.task)
	}
	return b
}

type frontend struct {
	name string

	// The StopChan in task is used to synchronize access to all fields
	task *golib.TCPListenerTask

	backends         []*backend
	counter          uint64
	backendEndpoints map[string]bool
}

type backend struct {
	endpoint string
	addr     *net.TCPAddr
}

func (b *TcpBalancer) AddBackend(frontendName, backendEndpoint string) error {
	frontend, ok := b.frontends[frontendName]
	if !ok {
		return fmt.Errorf("Cannot add backend endpoint %v: frontend %v not defined", backendEndpoint, frontendName)
	}
	return frontend.addBackend(backendEndpoint)
}

func (f *frontend) shutdown() {
	// Is called only after the frontend socket is already closed and no new connections are handled
	f.task.Execute(func() {
		for _, backend := range f.backends {
			backend.shutdown()
		}
	})
}

func (f *frontend) addBackend(endpoint string) (err error) {
	endpoint = strings.TrimSpace(endpoint)
	f.task.Execute(func() {
		if _, ok := f.backendEndpoints[endpoint]; ok {
			err = fmt.Errorf("Frontend %v already has the backend endpoint %v", f.name, endpoint)
		} else {
			addr, resolveErr := net.ResolveTCPAddr(TcpNet, endpoint)
			if resolveErr != nil {
				err = fmt.Errorf("Backend address %v could not be resolved: %v", endpoint, resolveErr)
			} else {
				b := &backend{
					endpoint: endpoint,
					addr:     addr,
				}
				f.backends = append(f.backends, b)
				f.backendEndpoints[endpoint] = true
			}
		}
	})
	return
}

// This function is already synchronized through f.task
func (f *frontend) handleConnection(wg *sync.WaitGroup, conn *net.TCPConn) {
	for i := 0; i < len(f.backends); i++ {
		// TODO implement different balancing strategies
		backend := f.backends[f.counter%len(f.backends)]
		f.counter++
		if err := backend.handleConnection(conn); err == nil {
			log.Debugln("[frontend %v from %v] Backend connection to %v failed: %v",
				f.name, conn.RemoteAddr(), backend.endpoint, err)
			return
		}
	}
	log.Errorln("Failed to select backend for '%v' connection from %v, no live backends (%v registered total)",
		f.name, conn.RemoteAddr(), len(f.backends))
	if err := conn.Close(); err != nil {
		log.Errorln("Failed to close connection:", err)
	}
}

func (b *backend) handleConnection(conn *net.TCPConn) error {
	backendConn, err := net.DialTCP(TcpNet, nil, b.addr)
	if err != nil {
		return err
	}
	b.startConnectionProxy(conn, backendConn)
	return nil
}

func (b *backend) shutdown() {
	// TODO mark as down, close all connections
}

func (b *backend) startConnectionProxy(frontendConn, backendConn *net.TCPConn) {

}
