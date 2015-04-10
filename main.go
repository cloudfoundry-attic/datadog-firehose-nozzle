package main
import (
    "crypto/tls"
    "github.com/cloudfoundry/noaa"
    "github.com/cloudfoundry/noaa/events"
    "os"
    "fmt"
)

func main() {

    if (len(os.Args) != 3) {
        fmt.Printf("Usage: %s <traffic controller url> <oauth token>", os.Args[0])
        return
    }

    url := os.Args[1]
    authToken := os.Args[2]

    consumer := noaa.NewConsumer(url, &tls.Config{InsecureSkipVerify: true}, nil)
    messages := make(chan *events.Envelope)
    errs := make(chan error)
    done := make(chan struct{})
    go consumer.Firehose("datadog-nozzle", authToken, messages, errs, done)
    for envelope := range messages {
        fmt.Printf("Received envelope: %s\n", envelope.String())
    }

}