package datadogclient_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestDatadogclient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Datadogclient Suite")
}
