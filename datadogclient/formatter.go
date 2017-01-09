package datadogclient

import "encoding/json"

type Formatter struct{}

func (f Formatter) Format(prefix string, maxPostBytes uint32, data map[MetricKey]MetricValue) [][]byte {
	var result [][]byte
	seriesBytes := formatMetrics(prefix, data)
	if uint32(len(seriesBytes)) > maxPostBytes {
		metricsA, metricsB := splitMetrics(data)
		result = append(result, f.Format(prefix, maxPostBytes, metricsA)...)
		result = append(result, f.Format(prefix, maxPostBytes, metricsB)...)
		return result
	}

	result = append(result, seriesBytes)
	return result
}

func formatMetrics(prefix string, data map[MetricKey]MetricValue) []byte {
	metrics := []Metric{}
	for key, mVal := range data {
		metrics = append(metrics, Metric{
			Metric: prefix + key.Name,
			Points: mVal.Points,
			Type:   "gauge",
			Tags:   mVal.Tags,
		})
	}

	encodedMetric, _ := json.Marshal(Payload{Series: metrics})
	return encodedMetric
}

func splitMetrics(data map[MetricKey]MetricValue) (a, b map[MetricKey]MetricValue) {
	a = make(map[MetricKey]MetricValue)
	b = make(map[MetricKey]MetricValue)
	for k, v := range data {
		split := len(v.Points) / 2
		a[k] = MetricValue{
			Tags:   v.Tags,
			Points: v.Points[:split],
		}
		b[k] = MetricValue{
			Tags:   v.Tags,
			Points: v.Points[split:],
		}
	}
	return a, b
}
