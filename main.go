package main

import (
	"fmt"
	"os"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/noaa/events"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("Usage: %s <traffic controller url> <oauth token> <datadog url>", os.Args[0])
		return
	}

	trafficControllerURL := os.Args[1]
	authToken := os.Args[2]
	dataDogURL := os.Args[3]

	consumer := noaa.NewConsumer(trafficControllerURL, nil, nil)
	messages := make(chan *events.Envelope)
	errs := make(chan error)
	done := make(chan struct{})
	go consumer.Firehose("datadog-nozzle", authToken, messages, errs, done)

	go func() {
		<-errs
		close(messages)
	}()

	var envelopes []*events.Envelope
	for envelope := range messages {
		envelopes = append(envelopes, envelope)
	}
	client := datadogclient.New(dataDogURL, "")
	err := client.PostTimeSeries(envelopes)
	if err != nil {
		panic(err)
	}
}
