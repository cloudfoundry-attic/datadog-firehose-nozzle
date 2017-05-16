## Summary

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

### Dependencies

We manage dependencies using Glide. So, in order to build, install or run tests, you should first [install glide](https://github.com/Masterminds/glide). Then, `glide install`. 

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

1. **When the nozzle receives a websocket Close frame with status `1008`, it publishes the value `1`.** Traffic Controller pings clients to determine if the connections are still alive. If it does not receive a Pong response before the KeepAlive deadline, it decides that the connection is too slow (or even dead) and sends the Close frame.

3. **Otherwise, the nozzle publishes `0`.**



### Tests

You need [ginkgo](http://onsi.github.io/ginkgo/) to run the tests. The tests can be executed by:
```
ginkgo -r

```

## Deploying

### [Bosh](http://bosh.io)

There is a bosh release that will configure, start and monitor the datadog nozzle:
[https://github.com/DataDog/datadog-firehose-nozzle-release](https://github.com/DataDog/datadog-firehose-nozzle-release
)
