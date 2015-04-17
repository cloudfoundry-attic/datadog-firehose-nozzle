package main

import (
	"fmt"

	"crypto/tls"
	"encoding/json"
	"flag"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/noaa/events"
	"io/ioutil"
	"log"
	"time"
)

type nozzleConfig struct {
	TrafficControllerURL  string
	DataDogURL            string
	DataDogAPIKey         string
	FlushDurationSeconds  uint32
	InsecureSSLSkipVerify bool
	MetricPrefix          string
}

func main() {
	var (
		configFilePath = flag.String("config", "config/datadog-firehose-nozzle.json", "Location of the nozzle config json file")
		oauthToken     = flag.String("token", "", "OAuth token to access the firehose")
	)
	flag.Parse()
	config, err := parseConfig(*configFilePath)

	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	trafficControllerURL := config.TrafficControllerURL
	authToken := *oauthToken
	dataDogURL := config.DataDogURL
	dataDogAPIKey := config.DataDogAPIKey
	flushDuration := config.FlushDurationSeconds

	consumer := noaa.NewConsumer(trafficControllerURL, &tls.Config{InsecureSkipVerify: config.InsecureSSLSkipVerify}, nil)
	messages := make(chan *events.Envelope)
	errs := make(chan error)
	done := make(chan struct{})
	go consumer.Firehose("datadog-nozzle", authToken, messages, errs, done)

	go func() {
		err := <-errs
		log.Printf("Error while reading from the firehose: %s", err.Error())
		close(done)
	}()

	client := datadogclient.New(dataDogURL, dataDogAPIKey, config.MetricPrefix)
	ticker := time.NewTicker(time.Duration(flushDuration) * time.Second)

	for {
		select {
		case <-ticker.C:
			postMetrics(client)
		case envelope := <-messages:
			client.AddMetric(envelope)
		case <-done:
			postMetrics(client)
			return
		}
	}
}

func parseConfig(configPath string) (nozzleConfig, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	var config nozzleConfig
	if err != nil {
		return config, fmt.Errorf("Can not read config file [%s]: %s", configPath, err)
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return config, fmt.Errorf("Can not parse config file %s: %s", configPath, err)
	}
	return config, err
}

func postMetrics(client *datadogclient.Client) {
	err := client.PostMetrics()
	if err != nil {
		log.Printf("Error: %s", err.Error())
	}
}
