package testhelpers

import (
	"bufio"
	"bytes"
	"sync"

	"github.com/cloudfoundry/gosteno"
)

type FakeBufferSink struct {
	writer *bufio.Writer
	codec  gosteno.Codec
	buffer *bytes.Buffer
	sync.Mutex
}

func NewFakeBufferSink(buffer *bytes.Buffer) *FakeBufferSink {
	writer := bufio.NewWriter(buffer)

	x := new(FakeBufferSink)
	x.writer = writer
	x.buffer = buffer
	return x
}

func (x *FakeBufferSink) AddRecord(record *gosteno.Record) {
	bytes, _ := x.codec.EncodeRecord(record)

	x.Lock()
	defer x.Unlock()

	x.writer.Write(bytes)

	// Need to append a newline for IO sink
	x.writer.WriteString("\n")
}

func (x *FakeBufferSink) Flush() {
	x.Lock()
	defer x.Unlock()

	x.writer.Flush()
}

func (x *FakeBufferSink) SetCodec(codec gosteno.Codec) {
	x.Lock()
	defer x.Unlock()

	x.codec = codec
}

func (x *FakeBufferSink) GetCodec() gosteno.Codec {
	x.Lock()
	defer x.Unlock()

	return x.codec
}

func (x *FakeBufferSink) GetContent() string {
	x.Lock()
	defer x.Unlock()
	return x.buffer.String()
}
