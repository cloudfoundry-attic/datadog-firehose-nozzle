package main

import (
	"flag"
	"log"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogfirehosenozzle"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/nozzleconfig"
)

func main() {
	var (
		configFilePath = flag.String("config", "config/datadog-firehose-nozzle.json", "Location of the nozzle config json file")
	)
	flag.Parse()

	config, err := nozzleconfig.Parse(*configFilePath)

	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	datadog_nozzle := datadogfirehosenozzle.NewDatadogFirehoseNozzle(config)
	datadog_nozzle.Start()
}
