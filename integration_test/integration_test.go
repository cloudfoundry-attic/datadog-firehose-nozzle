package integration_test

import (
	"encoding/json"
	"os/exec"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	. "github.com/cloudfoundry-incubator/datadog-firehose-nozzle/testhelpers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"os"
	"strings"
)

var _ = Describe("DatadogFirehoseNozzle", func() {
	var (
		fakeUAA        *FakeUAA
		fakeFirehose   *FakeFirehose
		fakeDatadogAPI *FakeDatadogAPI

		nozzleSession *gexec.Session
	)

	BeforeEach(func() {
		fakeUAA = NewFakeUAA("bearer", "123456789")
		fakeToken := fakeUAA.AuthToken()
		fakeFirehose = NewFakeFirehose(fakeToken)
		fakeDatadogAPI = NewFakeDatadogAPI()

		fakeUAA.Start()
		fakeFirehose.Start()
		fakeDatadogAPI.Start()

		os.Setenv("NOZZLE_FLUSHDURATIONSECONDS", "2")
		os.Setenv("NOZZLE_UAAURL", fakeUAA.URL())
		os.Setenv("NOZZLE_DATADOGURL", fakeDatadogAPI.URL())
		os.Setenv("NOZZLE_TRAFFICCONTROLLERURL", strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1))

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
		fakeUAA.Close()
		fakeFirehose.Close()
		fakeDatadogAPI.Close()
		nozzleSession.Kill().Wait()
	})

	It("forwards metrics in a batch", func(done Done) {

		fakeFirehose.AddEvent(events.Envelope{
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
		})

		fakeFirehose.AddEvent(events.Envelope{
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
		})

		fakeFirehose.AddEvent(events.Envelope{
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
		})

		// eventually receive a batch from fake DD
		var messageBytes []byte
		Eventually(fakeDatadogAPI.ReceivedContents, "2s").Should(Receive(&messageBytes))

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
				Expect(metric.Tags).To(HaveLen(2))
				Expect(metric.Tags[0]).To(HavePrefix("ip:"))
				Expect(metric.Tags[1]).To(HavePrefix("deployment:"))

				Expect(metric.Points).To(HaveLen(1))
				Expect(metric.Points[0].Value).To(Equal(3.0))
			} else if metric.Metric == "totalMetricsSent" {
				Expect(metric.Tags).To(HaveLen(2))
				Expect(metric.Tags[0]).To(HavePrefix("ip:"))
				Expect(metric.Tags[1]).To(HavePrefix("deployment:"))

				Expect(metric.Points).To(HaveLen(1))
				Expect(metric.Points[0].Value).To(Equal(0.0))
			} else if metric.Metric == "slowConsumerAlert" {

			} else {
				panic("Unknown metric " + metric.Metric)
			}
		}

		close(done)
	}, 2.0)
})
