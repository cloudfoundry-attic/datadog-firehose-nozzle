package datadogclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"errors"
	"github.com/cloudfoundry/sonde-go/events"
	"sync/atomic"
)

const DefaultAPIURL = "https://app.datadoghq.com/api/v1"

type Client struct {
	apiURL                string
	apiKey                string
	metricPoints          map[metricKey]metricValue
	prefix                string
	deployment            string
	ip                    string
	totalMessagesReceived uint64
	totalMetricsSent      uint64
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
	points []Point
}

type Payload struct {
	Series []Metric `json:"series"`
}

type Metric struct {
	Metric string   `json:"metric"`
	Points []Point  `json:"points"`
	Type   string   `json:"type"`
	Host   string   `json:"host,omitempty"`
	Tags   []string `json:"tags,omitempty"`
}

type Point struct {
	Timestamp int64
	Value     float64
}

func New(apiURL string, apiKey string, prefix string, deployment string, ip string) *Client {
	return &Client{
		apiURL:       apiURL,
		apiKey:       apiKey,
		metricPoints: make(map[metricKey]metricValue),
		prefix:       prefix,
		deployment:   deployment,
		ip:           ip,
	}
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	atomic.AddUint64(&c.totalMessagesReceived, 1)
	if envelope.GetEventType() != events.Envelope_ValueMetric && envelope.GetEventType() != events.Envelope_CounterEvent {
		return
	}

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
	mVal.points = append(mVal.points, Point{
		Timestamp: envelope.GetTimestamp() / int64(time.Second),
		Value:     value,
	})

	c.metricPoints[key] = mVal
}

func (c *Client) PostMetrics() error {
	numMetrics := len(c.metricPoints)
	log.Printf("Posting %d metrics", numMetrics)
	url := c.seriesURL()
	seriesBytes, formattedMetrics := c.formatMetrics()
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(seriesBytes))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return fmt.Errorf("datadog request returned HTTP status code: %v", resp.StatusCode)
	}

	atomic.AddUint64(&c.totalMetricsSent, formattedMetrics)
	c.metricPoints = make(map[metricKey]metricValue)

	return nil
}

func (c *Client) seriesURL() string {
	url := fmt.Sprintf("%s?api_key=%s", c.apiURL, c.apiKey)
	return url
}

func (c *Client) formatMetrics() ([]byte, uint64) {
	metrics := []Metric{}
	for key, mVal := range c.metricPoints {
		metrics = append(metrics, Metric{
			Metric: c.prefix + key.name,
			Points: mVal.points,
			Type:   "gauge",
			Tags:   mVal.tags,
		})
	}

	metrics = c.addInternalMetric(metrics, "totalMessagesReceived", &c.totalMessagesReceived)
	metrics = c.addInternalMetric(metrics, "totalMetricsSent", &c.totalMetricsSent)

	encodedMetric, _ := json.Marshal(Payload{Series: metrics})

	// + 2 for the two internal metrics
	return encodedMetric, uint64(len(c.metricPoints) + 2)
}

func (c *Client) addInternalMetric(metrics []Metric, name string, value *uint64) []Metric {
	return append(metrics, Metric{
		Metric: c.prefix + name,
		Points: []Point{
			Point{
				Timestamp: time.Now().Unix(),
				Value:     float64(atomic.LoadUint64(value)),
			},
		},
		Type: "gauge",
		Tags: []string{
			fmt.Sprintf("ip:%s", c.ip),
			fmt.Sprintf("deployment:%s", c.deployment),
		},
	})
}

func getName(envelope *events.Envelope) string {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetOrigin() + "." + envelope.GetValueMetric().GetName()
	case events.Envelope_CounterEvent:
		return envelope.GetOrigin() + "." + envelope.GetCounterEvent().GetName()
	default:
		panic("Unknown event type")
	}
}

func getValue(envelope *events.Envelope) float64 {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetValueMetric().GetValue()
	case events.Envelope_CounterEvent:
		return float64(envelope.GetCounterEvent().GetTotal())
	default:
		panic("Unknown event type")
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

func (p Point) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`[%d, %f]`, p.Timestamp, p.Value)), nil
}

func (p *Point) UnmarshalJSON(in []byte) error {
	var timestamp int64
	var value float64

	parsed, err := fmt.Sscanf(string(in), `[%d,%f]`, &timestamp, &value)
	if err != nil {
		return err
	}
	if parsed != 2 {
		return errors.New("expected two parsed values")
	}

	p.Timestamp = timestamp
	p.Value = value

	return nil
}
