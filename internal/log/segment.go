package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/takumi616/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// Represent a continuous sequence of records in the log.
// Include a store file (record bytes) and an index file (offset → position mappings).
type segment struct {
	store                  *store // The store file that holds the raw record bytes
	index                  *index // The index file that maps logical offsets to store positions
	baseOffset, nextOffset uint64 // baseOffset is the first offset in this segment, nextOffset is the next available one
	config                 Config // Configuration settings for size limits, etc.
}

// Initialize a new segment by creating or opening the store and index files.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	// Open or create the store file for this segment
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Open or create the index file for this segment
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Determine the nextOffset based on the index contents
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append a record to the segment and returns its offset.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	// Serialize the record using Protobuf
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// Append to store and get the position
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// Write offset (relative to baseOffset) and position to the index
	if err = s.index.Write(
		uint32(s.nextOffset-s.baseOffset),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

// Retrieve a record by its absolute offset.
func (s *segment) Read(off uint64) (*api.Record, error) {
	// Get the store position from index (using relative offset)
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// Read raw bytes from store
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// Deserialize into a Record
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// Return true if the segment has reached its maximum size.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close the segment's store and index files.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// Close and delete the segment's files from disk.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Returns the nearest multiple of k less than or equal to j.
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
