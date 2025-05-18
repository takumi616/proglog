package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// Constants defining the byte width of each part of an index entry
var (
	// Width of the logical offset (uint32)
	offWidth uint64 = 4
	// Width of the position in the store file (uint64)
	posWidth uint64 = 8
	// Total width of an index entry (12 bytes)
	entWidth = offWidth + posWidth
)

// Represent a memory-mapped index file that stores offset-position pairs.
type index struct {
	file *os.File    // File descriptor for the index file
	mmap gommap.MMap // Memory-mapped byte slice for the file
	size uint64      // Current size (in bytes) of valid index data
}

// Create and return a new index instance for the given file.
// Ensure the file is correctly sized and memory-mapped.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// Get current size of the file to track written bytes
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// Set the file size to index struct
	idx.size = uint64(fi.Size())

	// Extend or truncate file to max allowed index size
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// Memory-map the file with read/write access and shared mapping
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Close the index safely.
// Sync the mmap and file to disk, truncates unused space, and closes the file.
func (i *index) Close() error {
	// Flush memory-mapped data to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// Flush OS buffer to disk
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate file to actual written size (remove unused reserved space)
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Retrieve the offset and position from the index at the given entry number.
// If in == -1, it reads the last entry.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		// No entries in the index
		return 0, 0, io.EOF
	}

	// If -1, read the last entry
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// Calculate the byte position in the mmap
	bytePos := uint64(out) * entWidth

	// Ensure read does not exceed written data
	if i.size < bytePos+entWidth {
		return 0, 0, io.EOF
	}

	// Read logical offset (4 bytes) and position (8 bytes) from mmap
	offset := enc.Uint32(i.mmap[bytePos : bytePos+offWidth])
	position := enc.Uint64(i.mmap[bytePos+offWidth : bytePos+entWidth])
	return offset, position, nil
}

// Append a new offset-position pair to the index.
// Return an error if there is no space left.
func (i *index) Write(off uint32, pos uint64) error {
	// Check for available space in mmap
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Write offset and position to the memory-mapped region
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// Move the write cursor forward
	i.size += entWidth
	return nil
}

// Returns the name of the index file.
func (i *index) Name() string {
	return i.file.Name()
}
