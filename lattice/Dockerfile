FROM golang:1.4.2
MAINTAINER "pivotal-lamb"

RUN go get github.com/cloudfoundry-incubator/datadog-firehose-nozzle

CMD ["/go/bin/datadog-firehose-nozzle", "-config", "/go/src/github.com/cloudfoundry-incubator/datadog-firehose-nozzle/lattice/lattice-datadog-firehose-nozzle.json"]
