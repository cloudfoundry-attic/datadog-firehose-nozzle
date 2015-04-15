package main

import (
	"fmt"
	"os"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/noaa/events"
	"time"
	"strconv"
	"crypto/tls"
	"log"
)

func main() {
	if len(os.Args) != 6 {
		fmt.Printf("Usage: %s <traffic controller url> <oauth token> <datadog url> <datadog api key> <flush duration (s)>", os.Args[0])
		return
	}

	trafficControllerURL := os.Args[1]
	authToken := os.Args[2]
	dataDogURL := os.Args[3]
	dataDogApiKey := os.Args[4]
	flushDuration, err := strconv.Atoi(os.Args[5])
	if err != nil {
		fmt.Printf("Illegal value for flush duration: %s\n", os.Args[5])
		return
	}

	consumer := noaa.NewConsumer(trafficControllerURL, &tls.Config{InsecureSkipVerify: true}, nil)
	messages := make(chan *events.Envelope)
	errs := make(chan error)
	done := make(chan struct{})
	go consumer.Firehose("datadog-nozzle", authToken, messages, errs, done)

	go func() {
		err := <-errs
		fmt.Printf("Error while reading from the firehose: %s", err.Error())
	}()

	client := datadogclient.New(dataDogURL, dataDogApiKey)
	ticker := time.NewTicker(time.Duration(flushDuration) * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				err := client.PostMetrics()
				if err != nil {
					log.Printf("Error: %s", err.Error())
				}
			case <-done:
				return
			}
		}
	}()

	for envelope := range messages {
		client.AddMetric(envelope)
	}

	close(done)
}
