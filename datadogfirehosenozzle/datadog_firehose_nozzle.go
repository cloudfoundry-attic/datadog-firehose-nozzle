package datadogfirehosenozzle

import (
	"crypto/tls"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/nozzleconfig"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-golang/localip"
	"log"
	"time"
)

type DatadogFirehoseNozzle struct {
	config           *nozzleconfig.NozzleConfig
	done             chan struct{}
	messages         chan *events.Envelope
	authTokenFetcher AuthTokenFetcher
}

type AuthTokenFetcher interface {
	FetchAuthToken() string
}

func NewDatadogFirehoseNozzle(config *nozzleconfig.NozzleConfig, tokenFetcher AuthTokenFetcher) *DatadogFirehoseNozzle {
	return &DatadogFirehoseNozzle{
		config:           config,
		done:             make(chan struct{}),
		messages:         make(chan *events.Envelope),
		authTokenFetcher: tokenFetcher,
	}
}

func (d *DatadogFirehoseNozzle) Start() {
	var authToken string

	if !d.config.DisableAccessControl {
		authToken = d.authTokenFetcher.FetchAuthToken()
	}

	d.consumeFirehose(authToken)
	d.postToDatadog()
}

func (d *DatadogFirehoseNozzle) consumeFirehose(authToken string) {
	consumer := noaa.NewConsumer(
		d.config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: d.config.InsecureSSLSkipVerify},
		nil)

	errs := make(chan error)
	go consumer.Firehose(d.config.FirehoseSubscriptionID, authToken, d.messages, errs)
	go func() {
		err := <-errs
		log.Printf("Error while reading from the firehose: %s", err.Error())
		close(d.done)
		consumer.Close()
	}()
}

func (d *DatadogFirehoseNozzle) postToDatadog() {
	ipAddress, err := localip.LocalIP()
	if err != nil {
		panic(err)
	}

	client := datadogclient.New(d.config.DataDogURL, d.config.DataDogAPIKey, d.config.MetricPrefix, d.config.Deployment, ipAddress)
	ticker := time.NewTicker(time.Duration(d.config.FlushDurationSeconds) * time.Second)

	for {
		select {
		case <-ticker.C:
			postMetrics(client)
		case envelope := <-d.messages:
			client.AddMetric(envelope)
		case <-d.done:
			postMetrics(client)
			return
		}
	}
}

func postMetrics(client *datadogclient.Client) {
	err := client.PostMetrics()
	if err != nil {
		log.Printf("Error: %s", err.Error())
	}
}
