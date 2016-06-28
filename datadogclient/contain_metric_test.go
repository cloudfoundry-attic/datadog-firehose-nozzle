package datadogclient_test

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

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

type containMetricTags struct {
	name   string
	tags   []string
	series []datadogclient.Metric
}

func ContainMetricWithTags(name string, tags ...string) types.GomegaMatcher {
	sort.Strings(tags)
	return &containMetricTags{
		name: name,
		tags: tags,
	}
}

func (m *containMetricTags) Match(actual interface{}) (success bool, err error) {
	var ok bool
	m.series, ok = actual.([]datadogclient.Metric)
	if !ok {
		return false, errors.New("Actual must be of type []datadogclient.Metric")
	}
	for _, metric := range m.series {
		sort.Strings(metric.Tags)
		if metric.Metric == m.name && reflect.DeepEqual(metric.Tags, m.tags) {
			return true, nil
		}
	}
	return false, nil
}

func (m *containMetricTags) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected %#v to contain a metric named %s with tags %v", m.series, m.name, m.tags)
}

func (m *containMetricTags) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Did not expect %#v to contain a metric named %s with tags %v", m.series, m.name, m.tags)
}
