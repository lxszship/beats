package memqueue

import (
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingBuffer_InsertIntoRegionA(t *testing.T) {
	buf := newRingBuffer(nil, 4)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	insert(buf)
	buf.ack(2)
	succ, avail := insert(buf)

	assert.True(t, succ)
	assert.Equal(t, 0, avail)
}

func TestRingBuffer_CreateRegionBAndInsertIntoRegionB(t *testing.T) {
	buf := newRingBuffer(nil, 3)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	insert(buf)
	buf.ack(2)
	succ, avail := insert(buf)

	assert.True(t, succ)
	assert.Equal(t, 1, avail)
}

func TestRingBuffer_InsertIntoRegionB(t *testing.T) {
	buf := newRingBuffer(nil, 3)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	insert(buf)
	buf.ack(2)
	insert(buf)
	succ, avail := insert(buf)
	assert.True(t, succ)
	assert.Equal(t, 0, avail)
}

func TestRingBuffer_InsertOutOfCapacityWithoutRegionB(t *testing.T) {
	buf := newRingBuffer(nil, 0)
	insert := insertIncreaser()

	succ, avail := insert(buf)
	assert.True(t, !succ)
	assert.Equal(t, 0, avail)
}

func TestRingBuffer_InsertOutOfCapacityWithRegionB(t *testing.T) {
	buf := newRingBuffer(nil, 2)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	buf.ack(1)
	insert(buf)
	succ, avail := insert(buf)

	assert.True(t, !succ)
	assert.Equal(t, 0, avail)
}

func TestRingBuffer_Ack(t *testing.T) {
	buf := newRingBuffer(nil, 2)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	buf.ack(1)
	start, end := buf.activeBufferOffsets()
	assert.Equal(t, 1, start)
	assert.Equal(t, 2, end)
}

func TestRingBuffer_AckAndChangeRegion(t *testing.T) {
	buf := newRingBuffer(nil, 2)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	buf.ack(1)
	start, end := buf.activeBufferOffsets()
	assert.Equal(t, 1, start)
	assert.Equal(t, 2, end)
}

func TestRingBuffer_AckOutOfRegionA(t *testing.T) {
	assert.Panics(t, func() {
		buf := newRingBuffer(nil, 3)
		insert := insertIncreaser()
		insert(buf)
		insert(buf)
		insert(buf)
		buf.ack(2)
		insert(buf)
		buf.ack(2)
	})
}

func TestRingbuffer_Reserve(t *testing.T) {
	buf := newRingBuffer(nil, 3)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	insert(buf)
	start, events := buf.reserve(2)
	assert.Equal(t, 0, start)
	assert.Equal(t, 2, len(events))
}

func TestRingbuffer_ReserveBeyondRegionA(t *testing.T) {
	buf := newRingBuffer(nil, 2)
	insert := insertIncreaser()
	insert(buf)
	insert(buf)
	start, events := buf.reserve(3)
	assert.Equal(t, 0, start)
	assert.Equal(t, 2, len(events))
}

func TestRingBuffer(t *testing.T) {
	buf := newRingBuffer(nil, 3)
	buf.insert(publisher.Event{
		Content: beat.Event{
			Fields:     common.MapStr{
				"1": "1",
			},
		},
		Flags:   0,
	}, clientState{seq: 1})
	buf.insert(publisher.Event{
		Content: beat.Event{
			Fields:     common.MapStr{
				"2": "2",
			},
		},
		Flags:   0,
	}, clientState{seq: 2})
	buf.ack(1)
	buf.insert(publisher.Event{
		Content: beat.Event{
			Fields:     common.MapStr{
				"3": "3",
			},
		},
		Flags:   0,
	}, clientState{seq: 3})
	buf.insert(publisher.Event{
		Content: beat.Event{
			Fields:     common.MapStr{
				"4": "4",
			},
		},
		Flags:   0,
	}, clientState{seq: 4})
	buf.ack(1)
	buf.ack(2)
}


func insertIncreaser() func(buffer *ringBuffer) (bool, int) {
	var seq uint32 = 1
	return func(buffer *ringBuffer) (bool, int) {

		s := seq
		f := fmt.Sprintf("%d", seq)
		seq += 1

		return buffer.insert(publisher.Event{
			Content: beat.Event{
				Fields:     common.MapStr{
					f: f,
				},
			},
			Flags:   0,
		}, clientState{seq: s})

	}
}
