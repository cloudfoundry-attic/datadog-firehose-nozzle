# datadog-firehose-nozzle
CF component to forward metrics from the Loggregator Firehose to DataDog

# Running 
```
go run main.go -config config/datadog-firehose-nozzle.json -token "$(cf oauth-token | grep bearer)"
```
