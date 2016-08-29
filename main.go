package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogfirehosenozzle"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/logger"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/nozzleconfig"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/uaatokenfetcher"
)

var (
	logFilePath = flag.String("logFile", "", "The agent log file, defaults to STDOUT")
	logLevel    = flag.Bool("debug", false, "Debug logging")
	configFile  = flag.String("config", "config/datadog-firehose-nozzle.json", "Location of the nozzle config json file")
)

func main() {
	flag.Parse()

	log := logger.NewLogger(*logLevel, *logFilePath, "datadog-firehose-nozzle", "")

	config, err := nozzleconfig.Parse(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	tokenFetcher := uaatokenfetcher.New(
		config.UAAURL,
		config.Client,
		config.ClientSecret,
		config.InsecureSSLSkipVerify,
		log,
	)

	threadDumpChan := registerGoRoutineDumpSignalChannel()
	defer close(threadDumpChan)
	go dumpGoRoutine(threadDumpChan)

	log.Infof("Targeting datadog API URL: %s \n", config.DataDogURL)
	datadog_nozzle := datadogfirehosenozzle.NewDatadogFirehoseNozzle(config, tokenFetcher, log)
	datadog_nozzle.Start()
}

func registerGoRoutineDumpSignalChannel() chan os.Signal {
	threadDumpChan := make(chan os.Signal, 1)
	signal.Notify(threadDumpChan, syscall.SIGUSR1)

	return threadDumpChan
}

func dumpGoRoutine(dumpChan chan os.Signal) {
	for range dumpChan {
		goRoutineProfiles := pprof.Lookup("goroutine")
		if goRoutineProfiles != nil {
			goRoutineProfiles.WriteTo(os.Stdout, 2)
		}
	}
}
