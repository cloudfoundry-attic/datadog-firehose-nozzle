package datadogclient_test

import (
	"errors"
	"fmt"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	"github.com/onsi/gomega/types"
)

type containMetric struct {
	name   string
	series []datadogclient.Metric
	target *datadogclient.Metric
}

func ContainMetric(name string, target *datadogclient.Metric) types.GomegaMatcher {
	return &containMetric{
		name:   name,
		target: target,
	}
}

func (m *containMetric) Match(actual interface{}) (success bool, err error) {
	var ok bool
	m.series, ok = actual.([]datadogclient.Metric)
	if !ok {
		return false, errors.New("Actual must be of type []datadogclient.Metric")
	}
	for _, metric := range m.series {
		if metric.Metric == m.name {
			if m.target != nil {
				*m.target = metric
			}
			return true, nil
		}
	}
	return false, nil
}

func (m *containMetric) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected %#v to contain a metric named %s", m.series, m.name)
}

func (m *containMetric) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Did not expect %#v to contain a metric named %s", m.series, m.name)
}
