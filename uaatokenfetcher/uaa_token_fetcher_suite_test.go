package uaatokenfetcher_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestUaaTokenFetcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "UaaTokenFetcher Suite")
}
