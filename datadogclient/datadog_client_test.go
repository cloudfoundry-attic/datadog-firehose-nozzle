package datadogclient_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var bodyChan chan []byte

var _ = Describe("DatadogClient", func() {

	var ts *httptest.Server

	BeforeEach(func() {
		bodyChan = make(chan []byte, 1)
		ts = httptest.NewServer(http.HandlerFunc(handlePost))
	})

	It("posts ValueMetrics in JSON format", func() {
		c := datadogclient.New(ts.URL, "dummykey", "")

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(76),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`{
		"series":[
			{"metric":"origin.metricName",

				"points":[[1,5], [2,76]],
				"type":"gauge",
				"tags":["deployment:deployment-name", "job:doppler"]}
		]}`)))
	})

	It("registers metrics with the same name but different tags as different", func() {
		c := datadogclient.New(ts.URL, "dummykey", "")

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(76),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("gorouter"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))

		Expect(receivedBytes).To(ContainSubstring(`["deployment:deployment-name","job:doppler"]`))
		Expect(receivedBytes).To(ContainSubstring(`["deployment:deployment-name","job:gorouter"]`))
	})

	It("posts CounterEvents in JSON format and empties map after post", func() {
		c := datadogclient.New(ts.URL, "dummykey", "")

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(1),
				Total: proto.Uint64(5),
			},
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(6),
				Total: proto.Uint64(11),
			},
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`{
		"series":[
			{"metric":"origin.counterName",
				"points":[[1,5],[2,11]],
				"type":"gauge"}
		]}`)))

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`{"series":[]}`)))
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
