package datadogclient

import "encoding/json"

type Formatter struct{}

func (f Formatter) FormatMetrics(prefix string, maxPostBytes uint32, data map[MetricKey]MetricValue) [][]byte {
	if len(data) == 0 {
		return nil
	}

	var result [][]byte
	seriesBytes := formatMetrics(prefix, data)
	if uint32(len(seriesBytes)) > maxPostBytes && canSplit(data) {
		metricsA, metricsB := splitPoints(data)
		result = append(result, f.FormatMetrics(prefix, maxPostBytes, metricsA)...)
		result = append(result, f.FormatMetrics(prefix, maxPostBytes, metricsB)...)

		return result
	}

	result = append(result, seriesBytes)
	return result
}

func (f Formatter) FormatEvent(prefix string, maxPostBytes uint32, event Event) ([][]byte, error) {
	var result [][]byte
	eventsBytes, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return append(result, eventsBytes), err
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

func canSplit(data map[MetricKey]MetricValue) bool {
	for _, v := range data {
		if len(v.Points) > 1 {
			return true
		}
	}

	return false
}

func splitPoints(data map[MetricKey]MetricValue) (a, b map[MetricKey]MetricValue) {
	a = make(map[MetricKey]MetricValue)
	b = make(map[MetricKey]MetricValue)
	for k, v := range data {
		split := len(v.Points) / 2
		if split == 0 {
			a[k] = MetricValue{
				Tags:   v.Tags,
				Points: v.Points,
			}
			continue
		}

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
