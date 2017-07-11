package datadogclient_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/DataDog/datadog-firehose-nozzle/datadogclient"
)

var (
	bodies       [][]byte
	reqs         chan *http.Request
	responseCode int
	responseBody []byte
)

var _ = Describe("DatadogClient", func() {
	var (
		ts *httptest.Server
		c  *datadogclient.Client
	)

	BeforeEach(func() {
		bodies = nil
		reqs = make(chan *http.Request, 1000)
		responseCode = http.StatusOK
		responseBody = []byte("some-response-body")
		ts = httptest.NewServer(http.HandlerFunc(handlePost))
		c = datadogclient.New(
			ts.URL,
			"dummykey",
			"datadog.nozzle.",
			"test-deployment",
			"dummy-ip",
			time.Second,
			1024,
			gosteno.NewLogger("datadogclient test"),
		)
	})

	Context("datadog does not respond", func() {
		BeforeEach(func() {
			ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var nilChan chan struct{}
				<-nilChan
			}))
			c = datadogclient.New(
				ts.URL,
				"dummykey",
				"datadog.nozzle.",
				"test-deployment",
				"dummy-ip",
				time.Millisecond,
				1024,
				gosteno.NewLogger("datadogclient test"),
			)
		})

		It("respects the timeout", func() {
			c.AddMetric(&events.Envelope{
				Origin:    proto.String("test-origin"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_ValueMetric.Enum(),

				// fields that gets sent as tags
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
				Index:      proto.String("1"),
				Ip:         proto.String("10.0.1.2"),
			})

			errs := make(chan error)
			go func() {
				errs <- c.PostMetrics()
			}()
			Eventually(errs).Should(Receive(HaveOccurred()))
		})
	})

	It("sets Content-Type header when making POST requests", func() {
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("test-origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),

			// fields that gets sent as tags
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("1"),
			Ip:         proto.String("10.0.1.2"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		var req *http.Request
		Eventually(reqs).Should(Receive(&req))
		Expect(req.Method).To(Equal("POST"))
		Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
	})

	It("sends tags", func() {
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("test-origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),

			// fields that gets sent as tags
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("1"),
			Ip:         proto.String("10.0.1.2"),

			// additional tags
			Tags: map[string]string{
				"protocol":   "http",
				"request_id": "a1f5-deadbeef",
			},
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(4))

		var metric datadogclient.Metric
		Expect(payload.Series).To(ContainMetric("datadog.nozzle.test-origin.", &metric))
		Expect(metric.Tags).To(ConsistOf(
			"deployment:deployment-name",
			"job:doppler",
			"index:1",
			"ip:10.0.1.2",
			"protocol:http",
			"name:test-origin",
			"origin:test-origin",
			"request_id:a1f5-deadbeef",
		))
	})

	It("uses tags as an identifier for batching purposes", func() {
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("test-origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),

			// fields that gets sent as tags
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("1"),
			Ip:         proto.String("10.0.1.2"),

			// additional tags
			Tags: map[string]string{
				"protocol":   "http",
				"request_id": "a1f5-deadbeef",
			},
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("test-origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),

			// fields that gets sent as tags
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("1"),
			Ip:         proto.String("10.0.1.2"),

			// additional tags
			Tags: map[string]string{
				"protocol":   "https",
				"request_id": "d3ac-livefood",
			},
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(5))
		Expect(payload.Series).To(ContainMetricWithTags(
			"datadog.nozzle.test-origin.",
			"deployment:deployment-name",
			"index:1",
			"ip:10.0.1.2",
			"job:doppler",
			"name:test-origin",
			"origin:test-origin",
			"protocol:https",
			"request_id:d3ac-livefood",
		))
		Expect(payload.Series).To(ContainMetricWithTags(
			"datadog.nozzle.test-origin.",
			"deployment:deployment-name",
			"index:1",
			"ip:10.0.1.2",
			"job:doppler",
			"name:test-origin",
			"origin:test-origin",
			"protocol:http",
			"request_id:a1f5-deadbeef",
		))
	})

	It("ignores messages that aren't value metrics or counter events", func() {
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_LogMessage.Enum(),
			LogMessage: &events.LogMessage{
				Message:     []byte("log message"),
				MessageType: events.LogMessage_OUT.Enum(),
				Timestamp:   proto.Int64(1000000000),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ContainerMetric.Enum(),
			ContainerMetric: &events.ContainerMetric{
				ApplicationId: proto.String("app-id"),
				InstanceIndex: proto.Int32(4),
				CpuPercentage: proto.Float64(20.0),
				MemoryBytes:   proto.Uint64(19939949),
				DiskBytes:     proto.Uint64(29488929),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(3))

		validateMetrics(payload, 2, 0)
	})

	It("generates aggregate messages even when idle", func() {
		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(3))

		validateMetrics(payload, 0, 0)

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(2))
		err = json.Unmarshal(bodies[1], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(3))

		validateMetrics(payload, 0, 3)
	})

	It("posts ValueMetrics in JSON format", func() {
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

		Eventually(bodies).Should(HaveLen(1))

		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(4))

		metricFound := false
		for _, metric := range payload.Series {
			Expect(metric.Type).To(Equal("gauge"))

			if metric.Metric == "datadog.nozzle.origin.metricName" {
				metricFound = true
				Expect(metric.Points).To(Equal([]datadogclient.Point{
					datadogclient.Point{
						Timestamp: 1,
						Value:     5.0,
					},
					datadogclient.Point{
						Timestamp: 2,
						Value:     76.0,
					},
				}))
				Expect(metric.Tags).To(Equal([]string{
					"deployment:deployment-name",
					"job:doppler",
					"name:origin",
					"origin:origin",
				}))
			}
		}
		Expect(metricFound).To(BeTrue())

		validateMetrics(payload, 2, 0)
	})

	It("breaks up a message that exceeds the FlushMaxBytes", func() {
		for i := 0; i < 1000; i++ {
			c.AddMetric(&events.Envelope{
				Origin:    proto.String("origin"),
				Timestamp: proto.Int64(1000000000 + int64(i)),
				EventType: events.Envelope_ValueMetric.Enum(),
				ValueMetric: &events.ValueMetric{
					Name:  proto.String("metricName"),
					Value: proto.Float64(5),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
			})
		}

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		f := func() int {
			return len(bodies)
		}

		Eventually(f).Should(BeNumerically(">", 1))
	})

	It("discards metrics that exceed that max size", func() {
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String(strings.Repeat("some-big-name", 1000)),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		f := func() int {
			return len(bodies)
		}

		Consistently(f).Should(Equal(0))
	})

	It("registers metrics with the same name but different tags as different", func() {
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

		Eventually(bodies).Should(HaveLen(1))

		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(5))
		dopplerFound := false
		gorouterFound := false
		for _, metric := range payload.Series {
			Expect(metric.Type).To(Equal("gauge"))

			if metric.Metric == "datadog.nozzle.origin.metricName" {
				Expect(metric.Tags).To(HaveLen(4))
				Expect(metric.Tags[0]).To(Equal("deployment:deployment-name"))
				if metric.Tags[1] == "job:doppler" {
					dopplerFound = true
					Expect(metric.Points).To(Equal([]datadogclient.Point{
						datadogclient.Point{
							Timestamp: 1,
							Value:     5.0,
						},
					}))
				} else if metric.Tags[1] == "job:gorouter" {
					gorouterFound = true
					Expect(metric.Points).To(Equal([]datadogclient.Point{
						datadogclient.Point{
							Timestamp: 2,
							Value:     76.0,
						},
					}))
				} else {
					panic("Unknown tag found")
				}
			}
		}
		Expect(dopplerFound).To(BeTrue())
		Expect(gorouterFound).To(BeTrue())
		validateMetrics(payload, 2, 0)
	})

	It("posts CounterEvents in JSON format and empties map after post", func() {
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

		Eventually(bodies).Should(HaveLen(1))

		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(4))
		counterNameFound := false
		for _, metric := range payload.Series {
			Expect(metric.Type).To(Equal("gauge"))

			if metric.Metric == "datadog.nozzle.origin.counterName" {
				counterNameFound = true
				Expect(metric.Points).To(Equal([]datadogclient.Point{
					datadogclient.Point{
						Timestamp: 1,
						Value:     5.0,
					},
					datadogclient.Point{
						Timestamp: 2,
						Value:     11.0,
					},
				}))
			}
		}
		Expect(counterNameFound).To(BeTrue())
		validateMetrics(payload, 2, 0)

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(2))

		err = json.Unmarshal(bodies[1], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(3))

		validateMetrics(payload, 2, 4)
	})

	It("sends a value 1 for the slowConsumerAlert metric when consumer error is set", func() {
		c.AlertSlowConsumerError()

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(3))

		errMetric := findSlowConsumerMetric(payload)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Type).To(Equal("gauge"))
		Expect(errMetric.Points).To(HaveLen(1))
		Expect(errMetric.Points[0].Value).To(BeEquivalentTo(1))
	})

	It("sends a value 0 for the slowConsumerAlert metric when consumer error is not set", func() {
		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(3))

		errMetric := findSlowConsumerMetric(payload)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Type).To(Equal("gauge"))
		Expect(errMetric.Points).To(HaveLen(1))
		Expect(errMetric.Points[0].Value).To(BeEquivalentTo(0))
	})

	It("unsets the slow consumer error once it publishes the alert to datadog", func() {
		c.AlertSlowConsumerError()

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())

		errMetric := findSlowConsumerMetric(payload)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Points[0].Value).To(BeEquivalentTo(1))

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(2))
		err = json.Unmarshal(bodies[1], &payload)
		Expect(err).NotTo(HaveOccurred())

		errMetric = findSlowConsumerMetric(payload)
		Expect(findSlowConsumerMetric(payload)).ToNot(BeNil())
		Expect(errMetric.Points[0].Value).To(BeEquivalentTo(0))
	})

	It("returns an error when datadog responds with a non 200 response code", func() {
		responseCode = http.StatusBadRequest // 400
		responseBody = []byte("something went horribly wrong")
		err := c.PostMetrics()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("datadog request returned HTTP response: 400 Bad Request"))
		Expect(err.Error()).To(ContainSubstring("something went horribly wrong"))

		responseCode = http.StatusSwitchingProtocols // 101
		err = c.PostMetrics()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("datadog request returned HTTP response: 101"))

		responseCode = http.StatusAccepted // 201
		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
	})
})

func validateMetrics(payload datadogclient.Payload, totalMessagesReceived int, totalMetricsSent int) {
	totalMessagesReceivedFound := false
	totalMetricsSentFound := false
	for _, metric := range payload.Series {
		Expect(metric.Type).To(Equal("gauge"))

		internalMetric := false
		var metricValue int
		if metric.Metric == "datadog.nozzle.totalMessagesReceived" {
			totalMessagesReceivedFound = true
			internalMetric = true
			metricValue = totalMessagesReceived
		}
		if metric.Metric == "datadog.nozzle.totalMetricsSent" {
			totalMetricsSentFound = true
			internalMetric = true
			metricValue = totalMetricsSent
		}

		if internalMetric {
			Expect(metric.Points).To(HaveLen(1))
			Expect(metric.Points[0].Timestamp).To(BeNumerically(">", time.Now().Unix()-10), "Timestamp should not be less than 10 seconds ago")
			Expect(metric.Points[0].Value).To(Equal(float64(metricValue)))
			Expect(metric.Tags).To(Equal([]string{"ip:dummy-ip", "deployment:test-deployment"}))
		}
	}
	Expect(totalMessagesReceivedFound).To(BeTrue())
	Expect(totalMetricsSentFound).To(BeTrue())
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic("No body!")
	}

	reqs <- r
	bodies = append(bodies, body)
	w.WriteHeader(responseCode)
	w.Write(responseBody)
}

func findSlowConsumerMetric(payload datadogclient.Payload) *datadogclient.Metric {
	for _, metric := range payload.Series {
		if metric.Metric == "datadog.nozzle.slowConsumerAlert" {
			return &metric
		}
	}
	return nil
}
