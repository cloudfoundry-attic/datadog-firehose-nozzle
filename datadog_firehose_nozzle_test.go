package main_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os/exec"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"

	"log"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var (
	fakeFirehoseInputChan chan *events.Envelope
	fakeDDChan            chan []byte
)

var _ = Describe("DatadogFirehoseNozzle", func() {
	var (
		fakeUAA      *http.Server
		fakeFirehose *http.Server
		fakeDatadog  *http.Server

		nozzleSession *gexec.Session
	)

	BeforeEach(func() {
		fakeFirehoseInputChan = make(chan *events.Envelope)
		fakeDDChan = make(chan []byte)

		fakeUAA = &http.Server{
			Addr:    ":8084",
			Handler: http.HandlerFunc(fakeUAAHandler),
		}
		fakeFirehose = &http.Server{
			Addr:    ":8086",
			Handler: http.HandlerFunc(fakeFirehoseHandler),
		}
		fakeDatadog = &http.Server{
			Addr:    ":8087",
			Handler: http.HandlerFunc(fakeDatadogHandler),
		}

		go fakeUAA.ListenAndServe()
		go fakeFirehose.ListenAndServe()
		go fakeDatadog.ListenAndServe()

		var err error
		nozzleCommand := exec.Command(pathToNozzleExecutable, "-config", "fixtures/test-config.json")
		nozzleSession, err = gexec.Start(
			nozzleCommand,
			gexec.NewPrefixedWriter("[o][nozzle] ", GinkgoWriter),
			gexec.NewPrefixedWriter("[e][nozzle] ", GinkgoWriter),
		)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		nozzleSession.Kill().Wait()
	})

	It("forwards metrics in a batch", func(done Done) {
		fakeFirehoseInputChan <- &events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
				Unit:  proto.String("gauge"),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		}

		fakeFirehoseInputChan <- &events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(10),
				Unit:  proto.String("gauge"),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("gorouter"),
		}

		fakeFirehoseInputChan <- &events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(3000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(3),
				Total: proto.Uint64(15),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		}

		close(fakeFirehoseInputChan)

		// eventually receive a batch from fake DD
		var messageBytes []byte
		Eventually(fakeDDChan, "2s").Should(Receive(&messageBytes))

		// Break JSON blob into a list of blobs, one for each metric
		var payload datadogclient.Payload
		err := json.Unmarshal(messageBytes, &payload)
		Expect(err).NotTo(HaveOccurred())

		for _, metric := range payload.Series {
			Expect(metric.Type).To(Equal("gauge"))

			if metric.Metric == "origin.metricName" {
				Expect(metric.Tags).To(HaveLen(2))
				Expect(metric.Tags[0]).To(Equal("deployment:deployment-name"))
				if metric.Tags[1] == "job:doppler" {
					Expect(metric.Points).To(Equal([]datadogclient.Point{
						datadogclient.Point{
							Timestamp: 1,
							Value:     5.0,
						},
					}))
				} else if metric.Tags[1] == "job:gorouter" {
					Expect(metric.Points).To(Equal([]datadogclient.Point{
						datadogclient.Point{
							Timestamp: 2,
							Value:     10.0,
						},
					}))
				} else {
					panic("Unknown tag")
				}
			} else if metric.Metric == "origin.counterName" {
				Expect(metric.Tags).To(HaveLen(2))
				Expect(metric.Tags[0]).To(Equal("deployment:deployment-name"))
				Expect(metric.Tags[1]).To(Equal("job:doppler"))

				Expect(metric.Points).To(Equal([]datadogclient.Point{
					datadogclient.Point{
						Timestamp: 3,
						Value:     15.0,
					},
				}))
			} else if metric.Metric == "totalMessagesReceived" {
				Expect(metric.Tags).To(HaveLen(1))
				Expect(metric.Tags[0]).To(HavePrefix("ip:"))

				Expect(metric.Points).To(HaveLen(1))
				Expect(metric.Points[0].Value).To(Equal(3.0))
			} else {
				panic("Unknown metric " + metric.Metric)
			}
		}

		close(done)
	}, 2.0)
})

func fakeUAAHandler(rw http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	rw.Write([]byte(`
		{
			"token_type": "bearer",
			"access_token": "good-token"
		}
	`))
}

func fakeFirehoseHandler(rw http.ResponseWriter, r *http.Request) {
	defer GinkgoRecover()
	authorization := r.Header.Get("Authorization")

	if authorization != "bearer good-token" {
		log.Printf("Bad token passed to firehose: %s", authorization)
		rw.WriteHeader(403)
		r.Body.Close()
		return
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	ws, _ := upgrader.Upgrade(rw, r, nil)

	defer ws.Close()
	defer ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Time{})

	for envelope := range fakeFirehoseInputChan {
		buffer, err := proto.Marshal(envelope)
		Expect(err).NotTo(HaveOccurred())
		err = ws.WriteMessage(websocket.BinaryMessage, buffer)
		Expect(err).NotTo(HaveOccurred())
	}
}

func fakeDatadogHandler(rw http.ResponseWriter, r *http.Request) {
	contents, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	go func() {
		fakeDDChan <- contents
	}()
}
