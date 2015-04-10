package datadogclient_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/cloudfoundry/noaa/events"
	"github.com/gogo/protobuf/proto"
)

var bodyChan chan []byte

var _ = Describe("DatadogClient", func() {

	var ts *httptest.Server

	BeforeEach(func() {
		bodyChan = make(chan []byte, 1)
		ts = httptest.NewServer(http.HandlerFunc(handlePost))
	})

	It("Posts ValueMetrics in JSON format", func() {
		c := datadogclient.New(ts.URL, "dummykey")
		eventList := []events.Envelope{}

		eventList = append(eventList, events.Envelope{
			Origin: proto.String("origin"),
			Timestamp: proto.Int64(1),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
		})

		eventList = append(eventList, events.Envelope{
			Origin: proto.String("origin"),
			Timestamp: proto.Int64(2),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(76),
			},
		})

		err := c.PostTimeSeries(eventList)
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`{
		"series":[
			{"metric":"origin.metricName",
				"points":[[1,5.000000],[2,76.000000]],
				"type":"gauge",
				"tags":[]}
		]}`)))
	})

	It("Posts CounterEvents in JSON format", func() {
		c := datadogclient.New(ts.URL, "dummykey")
		eventList := []events.Envelope{}

		eventList = append(eventList, events.Envelope{
			Origin: proto.String("origin"),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(1),
				Total: proto.Uint64(5),
			},
			Timestamp: proto.Int64(1),
		})

		eventList = append(eventList, events.Envelope{
			Origin: proto.String("origin"),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(6),
				Total: proto.Uint64(11),
			},
			Timestamp: proto.Int64(2),
		})

		err := c.PostTimeSeries(eventList)
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`{
		"series":[
			{"metric":"origin.counterName",
				"points":[[1,5],[2,11]],
				"type":"gauge",
				"tags":[]}
		]}`)))
	})


})

func handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic("No body!")
	}

	bodyChan <- body
}
