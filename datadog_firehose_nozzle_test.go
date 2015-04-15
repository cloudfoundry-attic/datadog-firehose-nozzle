package main_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os/exec"
	"time"

	"github.com/cloudfoundry/noaa/events"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"

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
		fakeFirehose *http.Server
		fakeDatadog  *http.Server

		nozzleSession *gexec.Session
	)

	BeforeEach(func() {
		fakeFirehoseInputChan = make(chan *events.Envelope)
		fakeDDChan = make(chan []byte)

		fakeFirehose = &http.Server{
			Addr:    ":8086",
			Handler: http.HandlerFunc(fakeFirehoseHandler),
		}
		fakeDatadog = &http.Server{
			Addr:    ":8087",
			Handler: http.HandlerFunc(fakeDatadogHandler),
		}

		go fakeFirehose.ListenAndServe()
		go fakeDatadog.ListenAndServe()

		var err error
		nozzleCommand := exec.Command(pathToNozzleExecutable, "-config", "fixtures/test-config.json", "-token", "some-token")
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
			Tags: []*events.Tag{
				{Key: proto.String("deployment"), Value: proto.String("deployment-name")},
				{Key: proto.String("job"), Value: proto.String("doppler")},
			},
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
			Tags: []*events.Tag{
				{Key: proto.String("deployment"), Value: proto.String("deployment-name")},
				{Key: proto.String("job"), Value: proto.String("gorouter")},
			},
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
			Tags: []*events.Tag{
				{Key: proto.String("deployment"), Value: proto.String("deployment-name")},
				{Key: proto.String("job"), Value: proto.String("doppler")},
			},
		}

		close(fakeFirehoseInputChan)

		// eventually receive a batch from fake DD
		var messageBytes []byte
		Eventually(fakeDDChan, "2s").Should(Receive(&messageBytes))

		// Break JSON blob into a list of blobs, one for each metric
		var jsonBlob map[string][]interface{}

		err := json.Unmarshal(messageBytes, &jsonBlob)
		Expect(err).NotTo(HaveOccurred())
		var series [][]byte

		for _, metric := range jsonBlob["series"] {
			buffer, _ := json.Marshal(metric)
			series = append(series, buffer)
		}

		Expect(series).To(ConsistOf(
			MatchJSON(`{
                "metric":"origin.metricName",
                "points":[[1,5]],
                "type":"gauge",
                "tags":["deployment:deployment-name", "job:doppler"]
            }`),
			MatchJSON(`{
                "metric":"origin.metricName",
                "points":[[2,10]],
                "type":"gauge",
                "tags":["deployment:deployment-name", "job:gorouter"]
            }`),
			MatchJSON(`{
                "metric":"origin.counterName",
                "points":[[3,15]],
                "type":"gauge",
                "tags":["deployment:deployment-name", "job:doppler"]
            }`),
		))

		close(done)
	}, 2.0)
})

func fakeFirehoseHandler(rw http.ResponseWriter, r *http.Request) {
	defer GinkgoRecover()

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
