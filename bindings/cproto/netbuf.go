package cproto

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/restream/reindexer/bindings"
	"github.com/golang/snappy"
)

var bufPool sync.Pool

type NetBuffer struct {
	buf   []byte
	conn  *connection
	reqID int
	args  []interface{}
}

func (buf *NetBuffer) Fetch(ctx context.Context, offset, limit int, asJson bool) (err error) {
	flags := 0
	if asJson {
		flags |= bindings.ResultsJson
	} else {
		flags |= bindings.ResultsCJson | bindings.ResultsWithItemID
	}
	// fmt.Printf("cmdFetchResults(reqId=%d, offset=%d, limit=%d, json=%v, flags=%v)\n", buf.reqID, offset, limit, asJson, flags)
	netTimeout := uint32(buf.conn.owner.timeouts.RequestTimeout / time.Second)
	fetchBuf, err := buf.conn.rpcCall(ctx, cmdFetchResults, netTimeout, buf.reqID, flags, offset, limit)
	defer fetchBuf.Free()
	if err != nil {
		buf.close()
		return
	}
	fetchBuf.buf, buf.buf = buf.buf, fetchBuf.buf

	if err = buf.parseArgs(); err != nil {
		buf.close()
		return
	}
	if buf.args[1].(int) == -1 {
		buf.reqID = -1
	}
	return
}

func (buf *NetBuffer) Free() {
	if buf != nil {
		buf.close()
		bufPool.Put(buf)
	}
}

func (buf *NetBuffer) GetBuf() []byte {
	return buf.args[0].([]byte)
}

func (buf *NetBuffer) needClose() bool {
	return buf.reqID != -1
}

func (buf *NetBuffer) parseArgs() (err error) {
	if buf.args != nil {
		buf.args = buf.args[:0]
	}
	dec := newRPCDecoder(buf.buf)
	if err = dec.errCode(); err != nil {
		if rerr, ok := err.(bindings.Error); ok {
			if rerr.Code() == bindings.ErrTimeout {
				err = context.DeadlineExceeded
			} else if rerr.Code() == bindings.ErrCanceled {
				err = context.Canceled
			}
		}
		return
	}
	retCount := dec.argsCount()
	if retCount > 0 {
		for i := 0; i < retCount; i++ {
			buf.args = append(buf.args, dec.intfArg())
		}
	}
	return
}

func (buf *NetBuffer) close() {
	if buf.needClose() {
		netTimeout := uint32(buf.conn.owner.timeouts.RequestTimeout / time.Second)
		closeBuf, err := buf.conn.rpcCall(context.TODO(), cmdCloseResults, netTimeout, buf.reqID)
		buf.reqID = -1
		if err != nil {
			fmt.Printf("rx: query close error: %v", err)
		}
		closeBuf.Free()
	}
}

func newNetBuffer(size int, conn *connection) (buf *NetBuffer) {

	obj := bufPool.Get()
	if obj != nil {
		buf = obj.(*NetBuffer)
	} else {
		buf = &NetBuffer{}
	}
	if cap(buf.buf) >= size {
		buf.buf = buf.buf[:size]
	} else {
		buf.buf = make([]byte, size)
	}
	buf.conn = conn
	buf.reqID = -1
	if len(buf.args) > 0 {
		buf.args = buf.args[:0]
	}

	return buf
}

func (buf *NetBuffer) decompress() (err error) {
	buf.buf, err = snappy.Decode(nil, buf.buf)
	return err
}
