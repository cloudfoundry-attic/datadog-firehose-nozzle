package datadogfirehosenozzle

import (
	"crypto/tls"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/nozzleconfig"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
	"github.com/pivotal-golang/localip"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

type DatadogFirehoseNozzle struct {
	config           *nozzleconfig.NozzleConfig
	errs             chan error
	messages         chan *events.Envelope
	authTokenFetcher AuthTokenFetcher
	consumer         *noaa.Consumer
	client           *datadogclient.Client
}

type AuthTokenFetcher interface {
	FetchAuthToken() string
}

func NewDatadogFirehoseNozzle(config *nozzleconfig.NozzleConfig, tokenFetcher AuthTokenFetcher) *DatadogFirehoseNozzle {
	return &DatadogFirehoseNozzle{
		config:           config,
		errs:             make(chan error),
		messages:         make(chan *events.Envelope),
		authTokenFetcher: tokenFetcher,
	}
}

func (d *DatadogFirehoseNozzle) Start() {
	var authToken string

	if !d.config.DisableAccessControl {
		authToken = d.authTokenFetcher.FetchAuthToken()
	}

	d.createClient()

	d.consumeFirehose(authToken)
	d.postToDatadog()
}

func (d *DatadogFirehoseNozzle) createClient() {
	ipAddress, err := localip.LocalIP()
	if err != nil {
		panic(err)
	}

	d.client = datadogclient.New(d.config.DataDogURL, d.config.DataDogAPIKey, d.config.MetricPrefix, d.config.Deployment, ipAddress)
}

func (d *DatadogFirehoseNozzle) consumeFirehose(authToken string) {
	d.consumer = noaa.NewConsumer(
		d.config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: d.config.InsecureSSLSkipVerify},
		nil)

	go d.consumer.Firehose(d.config.FirehoseSubscriptionID, authToken, d.messages, d.errs)
}

func (d *DatadogFirehoseNozzle) postToDatadog() {
	ticker := time.NewTicker(time.Duration(d.config.FlushDurationSeconds) * time.Second)
	for {
		select {
		case <-ticker.C:
			d.postMetrics()
		case envelope := <-d.messages:
			d.client.AddMetric(envelope)
		case err := <-d.errs:
			d.handleError(err)
			return
		}
	}
}

func (d *DatadogFirehoseNozzle) postMetrics() {
	err := d.client.PostMetrics()
	if err != nil {
		log.Printf("Error: %s", err.Error())
	}
}

func (d *DatadogFirehoseNozzle) handleError(err error) {
	if err != io.EOF {
		log.Printf("Error while reading from the firehose: %v", err)

		if strings.Contains(err.Error(), strconv.Itoa(websocket.CloseInternalServerErr)) {
			log.Printf("Disconnected because nozzle couldn't keep up.")
			d.client.SetSlowConsumerError(err)
		}
	}
	d.consumer.Close()
	d.postMetrics()
}
