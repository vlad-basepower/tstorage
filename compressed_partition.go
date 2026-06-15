package tstorage

import (
	"bytes"
	"fmt"
	"time"
)

// A compressed partition implements a partition that holds Gorilla-encoded
// data points on the heap. It is read-only; it gets persisted to disk only
// by an explicit Storage.Flush call.
// The data is encoded in the same byte format as a disk partition's data file.
type compressedPartition struct {
	meta meta
	data []byte
	// duration to store data
	retention time.Duration
}

// compressMemoryPartition encodes all data points in the given memory partition
// into a compressed partition.
func compressMemoryPartition(m *memoryPartition, retention time.Duration, logger Logger) *compressedPartition {
	buf := &bytes.Buffer{}
	encoder := newSeriesEncoder(buf)

	metrics := map[string]diskMetric{}
	m.metrics.Range(func(key, value interface{}) bool {
		mt, ok := value.(*memoryMetric)
		if !ok {
			logger.Printf("unknown value found\n")
			return false
		}
		// The encoder writes to buf only on flush, so the current buffer
		// length is the offset the metric starts at.
		offset := int64(buf.Len())

		if err := mt.encodeAllPoints(encoder); err != nil {
			logger.Printf("failed to encode a data point that metric is %q: %v\n", mt.name, err)
			return false
		}

		if err := encoder.flush(); err != nil {
			logger.Printf("failed to flush data points that metric is %q: %v\n", mt.name, err)
			return false
		}

		totalNumPoints := mt.size + int64(len(mt.outOfOrderPoints))
		metrics[mt.name] = diskMetric{
			Name:          mt.name,
			Offset:        offset,
			MinTimestamp:  mt.minTimestamp,
			MaxTimestamp:  mt.maxTimestamp,
			NumDataPoints: totalNumPoints,
		}
		return true
	})

	return &compressedPartition{
		meta: meta{
			MinTimestamp:  m.minTimestamp(),
			MaxTimestamp:  m.maxTimestamp(),
			NumDataPoints: m.size(),
			Metrics:       metrics,
			CreatedAt:     time.Now(),
		},
		data:      buf.Bytes(),
		retention: retention,
	}
}

func (c *compressedPartition) insertRows(_ []Row) ([]Row, error) {
	return nil, fmt.Errorf("can't insert rows into compressed partition")
}

func (c *compressedPartition) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	if c.expired() {
		return nil, fmt.Errorf("this partition is expired: %w", ErrNoDataPoints)
	}
	return selectDataPointsFromBlock(c.data, c.meta, metric, labels, start, end)
}

func (c *compressedPartition) minTimestamp() int64 {
	return c.meta.MinTimestamp
}

func (c *compressedPartition) maxTimestamp() int64 {
	return c.meta.MaxTimestamp
}

func (c *compressedPartition) size() int {
	return c.meta.NumDataPoints
}

// Compressed partition is immutable.
func (c *compressedPartition) active() bool {
	return false
}

func (c *compressedPartition) clean() error {
	// Everything is on the heap and gets removed by GC.
	return nil
}

func (c *compressedPartition) expired() bool {
	return time.Since(c.meta.CreatedAt) > c.retention
}
