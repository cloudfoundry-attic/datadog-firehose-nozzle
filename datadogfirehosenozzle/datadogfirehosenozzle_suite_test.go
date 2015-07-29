package datadogfirehosenozzle_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"io/ioutil"
	"log"
	"testing"
)

func TestDatadogfirehosenozzle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DatadogFirehoseNozzle Suite")
}

var _ = BeforeSuite(func() {
	log.SetOutput(ioutil.Discard)
})
