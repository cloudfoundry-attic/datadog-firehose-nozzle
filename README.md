## Summary
[![Build Status](https://travis-ci.org/cloudfoundry-incubator/datadog-firehose-nozzle.svg?branch=master)](https://travis-ci.org/cloudfoundry-incubator/datadog-firehose-nozzle) [![Coverage Status](https://coveralls.io/repos/cloudfoundry-incubator/datadog-firehose-nozzle/badge.svg)](https://coveralls.io/r/cloudfoundry-incubator/datadog-firehose-nozzle)

The datadog-firehose-nozzle is a CF component which forwards metrics from the Loggregator Firehose to [Datadog](http://www.datadoghq.com/)

### Configure CloudFoundry UAA for Firehose Nozzle

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

### Running

The datadog nozzle uses a configuration file to obtain the firehose URL, datadog API key and other configuration parameters. The firehose and the datadog servers both require authentication -- the firehose requires a valid username/password and datadog requires a valid API key.

You can start the firehose nozzle by executing:
```
go run main.go -config config/datadog-firehose-nozzle.json"
```

### Batching

The configuration file specifies the interval at which the nozzle will flush metrics to datadog. By default this is set to 15 seconds.

### `slowConsumerAlert`
For the most part, the datadog-firehose-nozzle forwards metrics from the loggregator firehose to datadog without too much processing. A notable exception is the `datadog.nozzle.slowConsumerAlert` metric. The metric is a binary value (0 or 1) indicating whether or not the nozzle is forwarding metrics to datadog at the same rate that it is receiving them from the firehose: `0` means the the nozzle is keeping up with the firehose, and `1` means that the nozzle is falling behind.

The nozzle determines the value of `datadog.nozzle.slowConsumerAlert` with the following rules:

1. **When the nozzle receives a `TruncatingBuffer.DroppedMessages` metric, it publishes the value `1`.** The metric indicates that Doppler determined that the client (in this case, the nozzle) could not consume messages as quickly as the firehose was sending them, so it dropped messages from its queue of messages to send.

2. **When the nozzle receives a websocket Close frame with status `1008`, it publishes the value `1`.** Traffic Controller pings clients to determine if the connections are still alive. If it does not receive a Pong response before the KeepAlive deadline, it decides that the connection is too slow (or even dead) and sends the Close frame.

3. **Otherwise, the nozzle publishes `0`.**



### Tests

You need [ginkgo](http://onsi.github.io/ginkgo/) to run the tests. The tests can be executed by:
```
ginkgo -r

```

## Deploying

### [Bosh](http://bosh.io)

There is a bosh release that will configure, start and monitor the datadog nozzle:
[https://github.com/cloudfoundry-incubator/datadog-firehose-nozzle-release](https://github.com/cloudfoundry-incubator/datadog-firehose-nozzle-release
)

### [Lattice](http://lattice.cf)

There is a docker image which can be used to deploy the datadog nozzle to lattice.
If you are running lattice locally with Vagrant, you can use the following command
line to start the nozzle:

```bash
ltc create datadog-nozzle cloudfoundry/datadog-nozzle-lattice \
  -e NOZZLE_DATADOGAPIKEY=<API KEY> \
  -e NOZZLE_METRICPREFIX=<METRIC PREFIX>  --no-monitor
```

The `API KEY` is your datadog API key used to publish metrics. The `METRIC PREFIX` gets prepended to all metric names
going through the nozzle.

The docker image runs the nozzle with the config provided in [`lattice/lattice-datadog-firehose-nozzle.json`](https://github.com/cloudfoundry-incubator/datadog-firehose-nozzle/blob/master/lattice/lattice-datadog-firehose-nozzle.json).
If you are not running lattice locally you will have to also configure the traffic controller URL

```bash
ltc create datadog-nozzle cloudfoundry/datadog-nozzle-lattice \
  -e NOZZLE_DATADOGAPIKEY=<API KEY> \
  -e NOZZLE_METRIC_PREFIX=<METRIC PREFIX> \
  -e NOZZLE_TRAFFICCONTROLLERURL=<TRAFFICONTROLLER URL>
```

Any of the configuration parameters can be overloaded by using environment variables. The following
parameters are supported

| Environment variable          | Description            |
|-------------------------------|------------------------|
| NOZZLE_UAAURL                 | UAA URL which the nozzle uses to get an authentication token for the firehose |
| NOZZLE_USERNAME               | User who has access to the firehose |
| NOZZLE_PASSWORD               | Password for the user |
| NOZZLE_TRAFFICCONTROLLERURL   | Loggregator's traffic controller URL |
| NOZZLE_FIREHOSESUBSCRIPTIONID | Subscription ID used when connecting to the firehose. Nozzles with the same subscription ID get a proportional share of the firehose |
| NOZZLE_DATADOGURL             | The Datadog API URL |
| NOZZLE_DATADOGAPIKEY          | The API key used when publishing metrics to datadog |
| NOZZLE_METRICPREFIX           | The metric prefix is prepended to all metrics flowing through the nozzle |
| NOZZLE_DEPLOYMENT             | The deployment name for the nozzle. Used for tagging metrics internal to the nozzle |
| NOZZLE_FLUSHDURATIONSECONDS   | Number of seconds to buffer data before publishing to Datadog |
| NOZZLE_INSECURESSLSKIPVERIFY  | If true, allows insecure connections to the UAA and the Trafficcontroller |
| NOZZLE_DISABLEACCESSCONTROL   | If true, disables authentication with the UAA. Used in lattice deployments |
