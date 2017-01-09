package datadogclient_test

import (
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Formatter", func() {
	var (
		formatter datadogclient.Formatter
	)

	BeforeEach(func() {
		formatter = datadogclient.Formatter{}
	})

	It("does not return empty data", func() {
		result := formatter.Format("some-prefix", 1024, nil)
		Expect(result).To(HaveLen(0))
	})

	It("does not 'delete' points when trying to split", func() {
		m := make(map[datadogclient.MetricKey]datadogclient.MetricValue)
		m[datadogclient.MetricKey{Name: "a"}] = datadogclient.MetricValue{
			Points: []datadogclient.Point{{
				Value: 9,
			}},
		}
		result := formatter.Format("some-prefix", 1, m)

		Expect(result).To(HaveLen(1))
	})
})
