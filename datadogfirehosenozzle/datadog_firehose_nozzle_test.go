package datadogfirehosenozzle_test

import (
	. "github.com/cloudfoundry-incubator/datadog-firehose-nozzle/testhelpers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"encoding/json"
	"fmt"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogfirehosenozzle"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/nozzleconfig"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/uaatokenfetcher"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	"strings"
)

var _ = Describe("Datadogfirehosenozzle", func() {
	var fakeUAA *FakeUAA
	var fakeFirehose *FakeFirehose
	var fakeDatadogAPI *FakeDatadogAPI
	var config *nozzleconfig.NozzleConfig
	var nozzle *datadogfirehosenozzle.DatadogFirehoseNozzle

	BeforeEach(func() {
		fakeUAA = NewFakeUAA("bearer", "123456789")
		fakeToken := fakeUAA.AuthToken()
		fakeFirehose = NewFakeFirehose(fakeToken)
		fakeDatadogAPI = NewFakeDatadogAPI()

		fakeUAA.Start()
		fakeFirehose.Start()
		fakeDatadogAPI.Start()

		tokenFetcher := &uaatokenfetcher.UAATokenFetcher{
			UaaUrl: fakeUAA.URL(),
		}

		config = &nozzleconfig.NozzleConfig{
			UAAURL:               fakeUAA.URL(),
			FlushDurationSeconds: 10,
			DataDogURL:           fakeDatadogAPI.URL(),
			TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
			DisableAccessControl: false,
		}

		nozzle = datadogfirehosenozzle.NewDatadogFirehoseNozzle(config, tokenFetcher)
	})

	AfterEach(func() {
		fakeUAA.Close()
		fakeFirehose.Close()
		fakeDatadogAPI.Close()
	})

	It("Receives data from the firehose", func(done Done) {
		defer close(done)

		for i := 0; i < 10; i++ {
			envelope := events.Envelope{
				Origin:    proto.String("origin"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_ValueMetric.Enum(),
				ValueMetric: &events.ValueMetric{
					Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
					Value: proto.Float64(float64(i)),
					Unit:  proto.String("gauge"),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
			}
			fakeFirehose.AddEvent(envelope)
		}

		go nozzle.Start()

		var contents []byte
		Eventually(fakeDatadogAPI.ReceivedContents).Should(Receive(&contents))

		var payload datadogclient.Payload
		err := json.Unmarshal(contents, &payload)
		Expect(err).ToNot(HaveOccurred())

		// +2 internal metrics that show totalMessagesReceived and totalMetricSent
		Expect(payload.Series).To(HaveLen(12))

	}, 2)

	It("Gets a valid authentication token", func() {
		go nozzle.Start()
		Eventually(fakeFirehose.Requested).Should(BeTrue())
		Consistently(fakeFirehose.LastAuthorization).Should(Equal("bearer 123456789"))
	})

	Context("When the DisableAccessControl is set to true", func() {
		var tokenFetcher *FakeTokenFetcher

		BeforeEach(func() {
			fakeUAA = NewFakeUAA("", "")
			fakeToken := fakeUAA.AuthToken()
			fakeFirehose = NewFakeFirehose(fakeToken)
			fakeDatadogAPI = NewFakeDatadogAPI()
			tokenFetcher = &FakeTokenFetcher{}

			fakeUAA.Start()
			fakeFirehose.Start()
			fakeDatadogAPI.Start()

			config = &nozzleconfig.NozzleConfig{
				FlushDurationSeconds: 1,
				DataDogURL:           fakeDatadogAPI.URL(),
				TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: true,
			}

			nozzle = datadogfirehosenozzle.NewDatadogFirehoseNozzle(config, tokenFetcher)
		})

		AfterEach(func() {
			fakeUAA.Close()
			fakeFirehose.Close()
			fakeDatadogAPI.Close()
		})

		It("can still tries to connect to the firehose", func() {
			go nozzle.Start()
			Eventually(fakeFirehose.Requested).Should(BeTrue())
		})

		It("gets an empty authentication token", func() {
			go nozzle.Start()
			Consistently(fakeUAA.Requested).Should(Equal(false))
			Consistently(fakeFirehose.LastAuthorization).Should(Equal(""))
		})

		It("does not rquire the presence of config.UAAURL", func() {
			nozzle.Start()
			Consistently(func() int { return tokenFetcher.NumCalls }).Should(Equal(0))
		})
	})
})
