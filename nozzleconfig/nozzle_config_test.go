package nozzleconfig_test

import (
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/nozzleconfig"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
)

var _ = Describe("NozzleConfig", func() {
	BeforeEach(func() {
		os.Clearenv()
	})

	It("successfully parses a valid config", func() {
		conf, err := nozzleconfig.Parse("../config/datadog-firehose-nozzle.json")
		Expect(err).ToNot(HaveOccurred())
		Expect(conf.UAAURL).To(Equal("https://uaa.walnut.cf-app.com"))
		Expect(conf.Username).To(Equal("user"))
		Expect(conf.Password).To(Equal("user_password"))
		Expect(conf.DataDogURL).To(Equal("https://app.datadoghq.com/api/v1/series"))
		Expect(conf.DataDogAPIKey).To(Equal("<enter api key>"))
		Expect(conf.FlushDurationSeconds).To(BeEquivalentTo(15))
		Expect(conf.InsecureSSLSkipVerify).To(Equal(true))
		Expect(conf.MetricPrefix).To(Equal("datadogclient"))
		Expect(conf.Deployment).To(Equal("deployment-name"))
		Expect(conf.DisableAccessControl).To(Equal(false))
	})

	It("successfully overwrites file config values with environmental variables", func() {
		os.Setenv("NOZZLE_UAAURL", "https://uaa.walnut-env.cf-app.com")
		os.Setenv("NOZZLE_USERNAME", "env-user")
		os.Setenv("NOZZLE_PASSWORD", "env-user-password")
		os.Setenv("NOZZLE_DATADOGURL", "https://app.datadoghq-env.com/api/v1/series")
		os.Setenv("NOZZLE_DATADOGAPIKEY", "envapi-key>")
		os.Setenv("NOZZLE_FLUSHDURATIONSECONDS", "25")
		os.Setenv("NOZZLE_INSECURESSLSKIPVERIFY", "false")
		os.Setenv("NOZZLE_METRICPREFIX", "env-datadogclient")
		os.Setenv("NOZZLE_DEPLOYMENT", "env-deployment-name")
		os.Setenv("NOZZLE_DISABLEACCESSCONTROL", "true")

		conf, err := nozzleconfig.Parse("../config/datadog-firehose-nozzle.json")
		Expect(err).ToNot(HaveOccurred())
		Expect(conf.UAAURL).To(Equal("https://uaa.walnut-env.cf-app.com"))
		Expect(conf.Username).To(Equal("env-user"))
		Expect(conf.Password).To(Equal("env-user-password"))
		Expect(conf.DataDogURL).To(Equal("https://app.datadoghq-env.com/api/v1/series"))
		Expect(conf.DataDogAPIKey).To(Equal("envapi-key>"))
		Expect(conf.FlushDurationSeconds).To(BeEquivalentTo(25))
		Expect(conf.InsecureSSLSkipVerify).To(Equal(false))
		Expect(conf.MetricPrefix).To(Equal("env-datadogclient"))
		Expect(conf.Deployment).To(Equal("env-deployment-name"))
		Expect(conf.DisableAccessControl).To(Equal(true))
	})
})
