package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogfirehosenozzle"
)

func main() {
	var (
		configFilePath       = flag.String("config", "config/datadog-firehose-nozzle.json", "Location of the nozzle config json file")
		disableAccessControl = flag.Bool("disableAccessControl", false, "Disable access validation with UAA")
	)
	flag.Parse()
	config, err := parseConfig(*configFilePath)

	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	datadog_nozzle := datadogfirehosenozzle.NewDatadogFirehoseNozzle(config, *disableAccessControl)
	datadog_nozzle.Start()
}

func parseConfig(configPath string) (datadogfirehosenozzle.NozzleConfig, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	var config datadogfirehosenozzle.NozzleConfig
	if err != nil {
		return config, fmt.Errorf("Can not read config file [%s]: %s", configPath, err)
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return config, fmt.Errorf("Can not parse config file %s: %s", configPath, err)
	}
	return config, err
}
