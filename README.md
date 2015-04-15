# Summary
[![Build Status](https://travis-ci.org/cloudfoundry-incubator/datadog-firehose-nozzle.svg?branch=master)](https://travis-ci.org/cloudfoundry-incubator/datadog-firehose-nozzle)

The datadog-firehose-nozzle is a CF component which forwards metrics from the Loggregator Firehose to [Datadog](http://www.datadoghq.com/)

# Running

The datadog nozzle uses a configuration file to obtain the firehose URL, datadog API key and other configuration parameters. It also requires that you pass in a valid oauth-token for a CF user who has access to the firehose.

You can start the firehose nozzle by executing:
```
go run main.go -config config/datadog-firehose-nozzle.json -token "$(cf oauth-token | grep bearer)"
```

The above command-line assumes that you are logged into CF.


# Batching

The configuration file specifies the interval at which the nozzle will flush metrics to datadog. By default this is set to 15 seconds.

# Tests

You need [ginkgo](http://onsi.github.io/ginkgo/) to run the tests. The tests can be executed by:

```
ginkgo -r

```