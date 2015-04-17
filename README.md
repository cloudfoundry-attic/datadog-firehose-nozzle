# Summary
[![Build Status](https://travis-ci.org/cloudfoundry-incubator/datadog-firehose-nozzle.svg?branch=master)](https://travis-ci.org/cloudfoundry-incubator/datadog-firehose-nozzle) [![Coverage Status](https://coveralls.io/repos/cloudfoundry-incubator/datadog-firehose-nozzle/badge.svg)](https://coveralls.io/r/cloudfoundry-incubator/datadog-firehose-nozzle)

The datadog-firehose-nozzle is a CF component which forwards metrics from the Loggregator Firehose to [Datadog](http://www.datadoghq.com/)

# Configure CloudFoundry UAA for Firehose Nozzle

The datadog firehose nozzle requires a UAA user who is authorized to access the loggregator firehose. You can add a user by editing your CloudFoundry manifest to include the details about this user under the properties.uaa.clients section. For example to add a user `datadog-firehose-nozzle`:

```
properties:
  uaa:
    clients:
      datadog-firehose-nozzle:
        access-token-validity: 1209600
        authorized-grant-types: authorization_code,client_credentials,refresh_token
        override: true
        secret: <password>
        scope: openid,oauth.approvals,doppler.firehose
        authorities: oauth.login,doppler.firehose
```

# Running

The datadog nozzle uses a configuration file to obtain the firehose URL, datadog API key and other configuration parameters. The firehose and the datadog servers both require authentication -- the firehose requires a valid username/password and datadog requires a valid API key.

You can start the firehose nozzle by executing:
```
go run main.go -config config/datadog-firehose-nozzle.json"
```

# Batching

The configuration file specifies the interval at which the nozzle will flush metrics to datadog. By default this is set to 15 seconds.

# Tests

You need [ginkgo](http://onsi.github.io/ginkgo/) to run the tests. The tests can be executed by:
```
ginkgo -r

```
