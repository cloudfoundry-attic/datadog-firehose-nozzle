package datadogclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
)

const DefaultAPIURL = "https://app.datadoghq.com/api/v1"

type Client struct {
	apiURL       string
	apiKey       string
	metricPoints map[metricKey]metricValue
	prefix       string
}

func New(apiURL string, apiKey string, prefix string) *Client {
	return &Client{
		apiURL:       apiURL,
		apiKey:       apiKey,
		metricPoints: make(map[metricKey]metricValue),
		prefix:       prefix,
	}
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	key := metricKey{
		eventType:  envelope.GetEventType(),
		name:       getName(envelope),
		deployment: envelope.GetDeployment(),
		job:        envelope.GetJob(),
		index:      envelope.GetIndex(),
		ip:         envelope.GetIp(),
	}

	mVal := c.metricPoints[key]
	value := getValue(envelope)

	mVal.tags = getTags(envelope)
	mVal.points = append(mVal.points, point{
		timestamp: envelope.GetTimestamp() / int64(time.Second),
		value:     value,
	})

	c.metricPoints[key] = mVal
}

func (c *Client) PostMetrics() error {
	numMetrics := len(c.metricPoints)
	log.Printf("Posting %d metrics", numMetrics)
	url := c.seriesURL()
	seriesBytes := c.formatMetrics()
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(seriesBytes))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return fmt.Errorf("datadog request returned HTTP status code: %v", resp.StatusCode)
	}

	c.metricPoints = make(map[metricKey]metricValue)
	return nil
}

func (c *Client) seriesURL() string {
	url := fmt.Sprintf("%s?api_key=%s", c.apiURL, c.apiKey)
	return url
}

func (c *Client) formatMetrics() []byte {
	metrics := []metric{}
	for key, mVal := range c.metricPoints {
		metrics = append(metrics, metric{
			Metric: c.prefix + key.name,
			Points: mVal.points,
			Type:   "gauge",
			Tags:   mVal.tags,
		})
	}

	encodedMetric, _ := json.Marshal(payload{Series: metrics})

	return encodedMetric
}

type metricKey struct {
	eventType  events.Envelope_EventType
	name       string
	deployment string
	job        string
	index      string
	ip         string
}

type metricValue struct {
	tags   []string
	points []point
}

func getName(envelope *events.Envelope) string {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetOrigin() + "." + envelope.GetValueMetric().GetName()
	case events.Envelope_CounterEvent:
		return envelope.GetOrigin() + "." + envelope.GetCounterEvent().GetName()
	default:
		return ""
	}
}

func getValue(envelope *events.Envelope) float64 {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetValueMetric().GetValue()
	case events.Envelope_CounterEvent:
		return float64(envelope.GetCounterEvent().GetTotal())
	default:
		return 0
	}
}

func getTags(envelope *events.Envelope) []string {
	var tags []string

	tags = appendTagIfNotEmpty(tags, "deployment", envelope.GetDeployment())
	tags = appendTagIfNotEmpty(tags, "job", envelope.GetJob())
	tags = appendTagIfNotEmpty(tags, "index", envelope.GetIndex())
	tags = appendTagIfNotEmpty(tags, "ip", envelope.GetIp())

	return tags
}

func appendTagIfNotEmpty(tags []string, key string, value string) []string {
	if value != "" {
		tags = append(tags, fmt.Sprintf("%s:%s", key, value))
	}
	return tags
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
	Tags   []string `json:"tags,omitempty"`
}

type payload struct {
	Series []metric `json:"series"`
}
