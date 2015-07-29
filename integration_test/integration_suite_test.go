package integration_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"

	"github.com/onsi/gomega/gexec"
)

func TestDatadogFirehoseNozzle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var pathToNozzleExecutable string

var _ = BeforeSuite(func() {
	var err error
	pathToNozzleExecutable, err = gexec.Build("github.com/cloudfoundry-incubator/datadog-firehose-nozzle")
	Expect(err).ShouldNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})
