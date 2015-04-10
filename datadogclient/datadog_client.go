package datadogclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cloudfoundry/noaa/events"
)

const DefaultAPIURL = "https://app.datadoghq.com/api/v1"

type Client struct {
	apiURL string
	apiKey string
}

func New(apiURL string, apiKey string) *Client {
	return &Client{
		apiURL: apiURL,
		apiKey: apiKey,
	}
}

func (c *Client) PostTimeSeries(envelopes []events.Envelope) error {
	url := c.seriesURL()
	seriesBytes := formatMetrics(envelopes)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(seriesBytes))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("datadog request returned HTTP status code: %v", resp.StatusCode)
	}

	return nil
}

func (c *Client) seriesURL() string {
	url := fmt.Sprintf("%s/series?api_key=%s", c.apiURL, c.apiKey)
	return url
}

func formatMetrics(envelopes []events.Envelope) []byte {
	metricPoints := make(map[metricKey][]point)

	for _, envelope := range envelopes {
        key := metricKey{eventType: envelope.GetEventType(), name: getName(envelope)}

        points := metricPoints[key]

        value := getValue(envelope)

		points = append(points, point{
			timestamp: envelope.GetTimestamp(),
			value:     value,
		})

		metricPoints[key] = points
	}

	metrics := []metric{}
	for key, points := range metricPoints {
		metrics = append(metrics, metric{
			Metric: key.name,
			Points: points,
			Type:   "gauge",
			Tags:   []string{},
		})
	}

	encodedMetric, _ := json.Marshal(payload{Series: metrics})

	return encodedMetric
}

type metricKey struct {
	eventType events.Envelope_EventType
	name      string
}

func getName(envelope events.Envelope) string {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetOrigin() + "." + envelope.GetValueMetric().GetName()
	case events.Envelope_CounterEvent:
		return envelope.GetOrigin() + "." + envelope.GetCounterEvent().GetName()
	default:
		return ""
	}
}

func getValue(envelope events.Envelope) float64 {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetValueMetric().GetValue()
	case events.Envelope_CounterEvent:
		return float64(envelope.GetCounterEvent().GetTotal())
	default:
		return 0
	}
}

type point struct {
	timestamp int64
	value     float64
}

func (p point) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`[%d, %f]`, p.timestamp, p.value)), nil
}

type metric struct {
	Metric string   `json:"metric"`
	Points []point  `json:"points"`
	Type   string   `json:"type"`
	Host   string   `json:"host,omitempty"`
	Tags   []string `json:"tags"`
}

type payload struct {
	Series []metric `json:"series"`
}
