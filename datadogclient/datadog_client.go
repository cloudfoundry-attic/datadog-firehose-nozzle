package datadogclient

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"net/http"
	"sort"
	"time"

	"errors"

	"io/ioutil"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/sonde-go/events"
)

const DefaultAPIURL = "https://app.datadoghq.com/api/v1"

type Client struct {
	apiURL                string
	apiKey                string
	metricPoints          map[MetricKey]MetricValue
	prefix                string
	deployment            string
	ip                    string
	tagsHash              string
	totalMessagesReceived uint64
	totalMetricsSent      uint64
	httpClient            *http.Client
	maxPostBytes          uint32
	log                   *gosteno.Logger
	formatter             Formatter
}

type MetricKey struct {
	EventType events.Envelope_EventType
	Name      string
	TagsHash  string
}

type MetricValue struct {
	Tags   []string
	Points []Point
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

func New(
	apiURL string,
	apiKey string,
	prefix string,
	deployment string,
	ip string,
	writeTimeout time.Duration,
	maxPostBytes uint32,
	log *gosteno.Logger,
) *Client {
	ourTags := []string{
		"deployment:" + deployment,
		"ip:" + ip,
	}

	httpClient := &http.Client{
		Timeout: writeTimeout,
	}

	return &Client{
		apiURL:       apiURL,
		apiKey:       apiKey,
		metricPoints: make(map[MetricKey]MetricValue),
		prefix:       prefix,
		deployment:   deployment,
		ip:           ip,
		log:          log,
		tagsHash:     hashTags(ourTags),
		httpClient:   httpClient,
		maxPostBytes: maxPostBytes,
		formatter:    Formatter{},
	}
}

func (c *Client) AlertSlowConsumerError() {
	c.addInternalMetric("slowConsumerAlert", uint64(1))
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	c.totalMessagesReceived++
	if envelope.GetEventType() != events.Envelope_ValueMetric && envelope.GetEventType() != events.Envelope_CounterEvent {
		return
	}

	tags := parseTags(envelope)
	key := MetricKey{
		EventType: envelope.GetEventType(),
		Name:      getName(envelope),
		TagsHash:  hashTags(tags),
	}

	mVal := c.metricPoints[key]
	value := getValue(envelope)

	mVal.Tags = tags
	mVal.Points = append(mVal.Points, Point{
		Timestamp: envelope.GetTimestamp() / int64(time.Second),
		Value:     value,
	})

	c.metricPoints[key] = mVal
}

func (c *Client) PostMetrics() error {
	c.populateInternalMetrics()
	numMetrics := len(c.metricPoints)
	c.log.Infof("Posting %d metrics", numMetrics)

	c.totalMetricsSent += uint64(len(c.metricPoints))
	seriesBytes := c.formatter.Format(c.prefix, c.maxPostBytes, c.metricPoints)
	c.metricPoints = make(map[MetricKey]MetricValue)

	for _, data := range seriesBytes {
		if err := c.postMetrics(data); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) postMetrics(seriesBytes []byte) error {
	url := c.seriesURL()
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(seriesBytes))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			body = []byte("failed to read body")
		}
		return fmt.Errorf("datadog request returned HTTP response: %s\nResponse Body: %s", resp.Status, body)
	}

	return nil
}

func (c *Client) seriesURL() string {
	url := fmt.Sprintf("%s?api_key=%s", c.apiURL, c.apiKey)
	return url
}

func (c *Client) populateInternalMetrics() {
	c.addInternalMetric("totalMessagesReceived", c.totalMessagesReceived)
	c.addInternalMetric("totalMetricsSent", c.totalMetricsSent)

	if !c.containsSlowConsumerAlert() {
		c.addInternalMetric("slowConsumerAlert", uint64(0))
	}
}

func (c *Client) containsSlowConsumerAlert() bool {
	key := MetricKey{
		Name:     "slowConsumerAlert",
		TagsHash: c.tagsHash,
	}
	_, ok := c.metricPoints[key]
	return ok
}

func (c *Client) addInternalMetric(name string, value uint64) {
	key := MetricKey{
		Name:     name,
		TagsHash: c.tagsHash,
	}

	point := Point{
		Timestamp: time.Now().Unix(),
		Value:     float64(value),
	}

	mValue := MetricValue{
		Tags: []string{
			fmt.Sprintf("ip:%s", c.ip),
			fmt.Sprintf("deployment:%s", c.deployment),
		},
		Points: []Point{point},
	}

	c.metricPoints[key] = mValue
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

func parseTags(envelope *events.Envelope) []string {
	tags := appendTagIfNotEmpty(nil, "deployment", envelope.GetDeployment())
	tags = appendTagIfNotEmpty(tags, "job", envelope.GetJob())
	tags = appendTagIfNotEmpty(tags, "index", envelope.GetIndex())
	tags = appendTagIfNotEmpty(tags, "ip", envelope.GetIp())
	for tname, tvalue := range envelope.GetTags() {
		tags = appendTagIfNotEmpty(tags, tname, tvalue)
	}
	return tags
}

func appendTagIfNotEmpty(tags []string, key, value string) []string {
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

func hashTags(tags []string) string {
	sort.Strings(tags)
	hash := ""
	for _, tag := range tags {
		tagHash := sha1.Sum([]byte(tag))
		hash += string(tagHash[:])
	}
	return hash
}
