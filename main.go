package main

import (
	"flag"
	"log"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogfirehosenozzle"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/nozzleconfig"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/uaatokenfetcher"
)

func main() {
	configFilePath := flag.String("config", "config/datadog-firehose-nozzle.json", "Location of the nozzle config json file")
	flag.Parse()

	config, err := nozzleconfig.Parse(*configFilePath)
	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	tokenFetcher := &uaatokenfetcher.UAATokenFetcher{
		UaaUrl:                config.UAAURL,
		Username:              config.Username,
		Password:              config.Password,
		InsecureSSLSkipVerify: config.InsecureSSLSkipVerify,
	}
	datadog_nozzle := datadogfirehosenozzle.NewDatadogFirehoseNozzle(config, tokenFetcher)
	datadog_nozzle.Start()
}
