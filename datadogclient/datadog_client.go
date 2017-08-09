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
	events                []Event
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

type Event struct {
	Title          string   `json:"title"`
	Text           string   `json:"text"`
	DateHappened   int64    `json:"date_happened,omitempty"`
	Host           string   `json:"host,omitempty"`
	Priority       string   `json:"priority,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	AlertType      string   `json:"alert_type,omitempty"`
	AggregationKey string   `json:"aggregation_key,omitempty"`
	SourceTypeName string   `json:"source_type_name,omitempty"`
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

func (c *Client) Add(envelope *events.Envelope) {
	c.totalMessagesReceived++

	switch envelope.GetEventType() {
	case events.Envelope_CounterEvent, events.Envelope_ValueMetric:
		c.addMetric(envelope)
	case events.Envelope_Error:
		c.addEvent(envelope)
	default:
		return
	}
}

func (c *Client) addMetric(envelope *events.Envelope) {
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

func (c *Client) addEvent(envelope *events.Envelope) {
	title := fmt.Sprintf("%s: %d", envelope.GetError().GetSource(), envelope.GetError().GetCode())
	event := Event{
		Title:        title,
		Text:         envelope.GetError().GetMessage(),
		DateHappened: envelope.GetTimestamp() / int64(time.Second),
		Tags:         parseTags(envelope),
	}

	c.events = append(c.events, event)
}

func (c *Client) Post() error {
	c.populateInternalMetrics()

	if err := c.postMetrics(); err != nil {
		return err
	}

	if err := c.postEvents(); err != nil {
		return err
	}

	return nil
}

func (c *Client) postMetrics() error {
	numMetrics := len(c.metricPoints)
	c.log.Infof("Posting %d metrics", numMetrics)

	c.totalMetricsSent += uint64(len(c.metricPoints))
	seriesBytes := c.formatter.FormatMetrics(c.prefix, c.maxPostBytes, c.metricPoints)
	c.metricPoints = make(map[MetricKey]MetricValue)

	return c.sendData(seriesBytes, c.seriesURL())
}

func (c *Client) postEvents() error {
	numEvents := len(c.events)
	c.log.Infof("Posting %d events", numEvents)

	c.totalMetricsSent += uint64(numEvents)
	for _, event := range c.events {
		eventBytes, err := c.formatter.FormatEvent(c.prefix, c.maxPostBytes, event)
		if err != nil {
			c.log.Warn("Cannot format event")
			continue
		}

		if err := c.sendData(eventBytes, c.eventsURL()); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) sendData(data [][]byte, url string) error {
	for _, chunk := range data {
		if uint32(len(chunk)) > c.maxPostBytes {
			c.log.Infof("Throwing out data chunk that exceeds %d bytes", c.maxPostBytes)
			continue
		}

		if err := c.send(chunk, url); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) send(data []byte, url string) error {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
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
	url := fmt.Sprintf("%s/series?api_key=%s", c.apiURL, c.apiKey)
	return url
}

func (c *Client) eventsURL() string {
	url := fmt.Sprintf("%s/events?api_key=%s", c.apiURL, c.apiKey)
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
