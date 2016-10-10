package testhelpers

import (
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/gorilla/websocket"
)

type FakeIdleFirehose struct {
	server      *httptest.Server
	idleTimeout time.Duration
	done        chan struct{}
}

func NewFakeIdleFirehose(timeout time.Duration) *FakeIdleFirehose {
	return &FakeIdleFirehose{idleTimeout: timeout, done: make(chan struct{})}
}

func (f *FakeIdleFirehose) Start() {
	f.server = httptest.NewUnstartedServer(f)
	f.server.Start()
}

func (f *FakeIdleFirehose) Close() {
	close(f.done)
	f.server.Close()
}

func (f *FakeIdleFirehose) URL() string {
	return f.server.URL
}

func (f *FakeIdleFirehose) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	ws, _ := upgrader.Upgrade(w, r, nil)

	defer ws.Close()
	defer ws.WriteControl(websocket.CloseMessage, []byte{}, time.Time{})

	timer := time.NewTimer(f.idleTimeout)
	select {
	case <-timer.C:
	case <-f.done:
	}
}
