package datadogclient_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"io/ioutil"
	"log"
	"testing"
)

func TestDatadogclient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DatadogClient Suite")
}

var _ = BeforeSuite(func() {
	log.SetOutput(ioutil.Discard)
})
