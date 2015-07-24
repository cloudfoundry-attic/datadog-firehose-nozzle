package testhelpers

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
)

type FakeDatadogAPI struct {
	server           *httptest.Server
	ReceivedContents chan []byte
}

func NewFakeDatadogAPI() *FakeDatadogAPI {
	return &FakeDatadogAPI{
		ReceivedContents: make(chan []byte, 100),
	}
}

func (f *FakeDatadogAPI) Start() {
	f.server = httptest.NewUnstartedServer(f)
	f.server.Start()
}

func (f *FakeDatadogAPI) Close() {
	f.server.Close()
}

func (f *FakeDatadogAPI) URL() string {
	return f.server.URL
}

func (f *FakeDatadogAPI) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	contents, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	go func() {
		f.ReceivedContents <- contents
	}()
}
