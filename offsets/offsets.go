/*
  Utility functions for managing starting and out-of-range offsets.

  These are useful in conjunction with the sarama-consumer.Config.StartingOffset
  and .OffsetOutOfRange configuration options.

  Copyright 2017 MistSys
*/

package offsets

import (
	"time"

	"github.com/Shopify/sarama"
	consumer "github.com/mistsys/sarama-consumer"
)

func logf(fmt string, args ...interface{}) {
	consumer.Logf(fmt, args...)
}

// return a StartingOffset function which starts the consuming at no more than 'max' time in the past
// and a OffsetOutOfRange function which jumps forward to no more than 'max' time in the past
func NoOlderThan(max time.Duration) (consumer.StartingOffset, consumer.OffsetOutOfRange) {
	if max > 0 {
		return func(topic string, partition int32, committed_offset int64, client sarama.Client) (offset int64, err error) {
				logf("StartingOffset(%q, %d, %d) no older than %v requested", topic, partition, committed_offset, max)
				offset = committed_offset
				if offset == sarama.OffsetNewest {
					// start with the sarama configured starting point
					offset = client.Config().Consumer.Offsets.Initial
					logf("using Config.Consumer.Offsets.Initial %d", offset)
				}
				ts := time.Now().UTC().Add(-max)
				ts_msec := ts.UnixNano() / 1000000 // millisecond timestamp of oldest time at which we want to find ourselves
				ts_offset, err := client.GetOffset(topic, partition, ts_msec)
				logf("GetOffset(%q,%d,%v) -> %d, %v", topic, partition, ts, ts_offset, err)
				if err == sarama.ErrOffsetOutOfRange {
					logf("StartingOffset(%q, %d, %d) returning %d", topic, partition, committed_offset, offset)
					return offset, nil
				}
				if err != nil {
					logf("StartingOffset(%q, %d, %d) returning %d, %v", topic, partition, committed_offset, offset, err)
					return offset, err
				}
				if offset < ts_offset { // NOTE this works correctly when offset is sarama.OffsetNewest or OffsetOldest, both of which are negative
					// jump ahead to ts_offset
					offset = ts_offset
				}
				logf("StartingOffset(%q, %d, %d) returning %d", topic, partition, committed_offset, offset)
				return offset, nil
			},
			func(topic string, partition int32, client sarama.Client) (offset int64, err error) {
				logf("OffsetOfOfRange(%q, %d) no older than %v requested", topic, partition, max)
				ts := time.Now().UTC().Add(-max)
				ts_msec := ts.UnixNano() / 1000000 // millisecond timestamp of oldest time at which we want to find ourselves
				ts_offset, err := client.GetOffset(topic, partition, ts_msec)
				logf("GetOffset(%q,%d,%v) -> %d, %v", topic, partition, ts, ts_offset, err)
				if err == sarama.ErrOffsetOutOfRange {
					// max time ago is too far back (assuming the local time is valid). so start at the oldest offset
					logf("OffsetOfOfRange(%q, %d) returning OffsetOldest", topic, partition)
					return sarama.OffsetOldest, nil
				}
				logf("OffsetOfOfRange(%q, %d) returning %d", topic, partition, ts_offset)
				return ts_offset, err
			}
	}

	// max is 0 (or negative). immediately jump to sarama.OffsetNewest
	return func(topic string, partition int32, committed_offset int64, client sarama.Client) (int64, error) {
			logf("StartingOffset(%q, %d, %d) hardcoded to OffsetNewest", topic, partition, committed_offset)
			return sarama.OffsetNewest, nil
		},
		func(topic string, partition int32, client sarama.Client) (int64, error) {
			logf("OffsetOfOfRange(%q, %d) hardcoded to OffsetNewest", topic, partition)
			return sarama.OffsetNewest, nil
		}
}

// A function which returns the oldest offset as the starting offset.
func offsetOldest() (consumer.StartingOffset) {

	return func(topic string, partition int32, committed_offset int64, client sarama.Client) (offset int64, err error) {
		return sarama.OffsetOldest, nil
	}
}
