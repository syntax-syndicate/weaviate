package diskann

import (
	"encoding/binary"
	"math"
	"os"
	"syscall"
)

type DiskSegment struct {
	Id        uint64
	Vector    []float32
	Neighbors []uint64
}

var idSize = int64(8) // uint64 for ID

func WriteSegmentsToDisk(segments []*VamanaSegment, chunkSize int64, mf *MappedFile, vectorLenSize int64, neighborLenSize int64) error {
	for i, segment := range segments {
		offset := int64(i) * chunkSize

		// Write ID
		binary.LittleEndian.PutUint64(mf.data[offset:], segment.id)
		offset += idSize

		// Write vector length
		binary.LittleEndian.PutUint32(mf.data[offset:], uint32(len(segment.vector)))
		offset += vectorLenSize

		// Write vector data
		for _, v := range segment.vector {
			binary.LittleEndian.PutUint32(mf.data[offset:], math.Float32bits(v))
			offset += 4
		}

		// Write neighbors length
		binary.LittleEndian.PutUint32(mf.data[offset:], uint32(len(segment.neighbors)))
		offset += neighborLenSize

		// Write neighbors
		for _, neighbor := range segment.neighbors {
			binary.LittleEndian.PutUint64(mf.data[offset:], neighbor.id)
			offset += 8
		}
	}

	return mf.file.Sync()
}

func WriteSegmentToDisk(segment DiskSegment, chunkSize int64, mf *MappedFile, vectorLenSize int64, neighborLenSize int64) error {
	offset := int64(segment.Id) * chunkSize

	if offset+chunkSize > mf.size {
		return os.ErrInvalid
	}

	// Write ID
	binary.LittleEndian.PutUint64(mf.data[offset:], segment.Id)
	offset += idSize

	// Write vector length
	binary.LittleEndian.PutUint32(mf.data[offset:], uint32(len(segment.Vector)))
	offset += vectorLenSize

	// Write vector data
	for _, v := range segment.Vector {
		binary.LittleEndian.PutUint32(mf.data[offset:], math.Float32bits(v))
		offset += 4
	}

	// Write neighbors length
	binary.LittleEndian.PutUint32(mf.data[offset:], uint32(len(segment.Neighbors)))
	offset += neighborLenSize

	// Write neighbors
	for _, neighbor := range segment.Neighbors {
		binary.LittleEndian.PutUint64(mf.data[offset:], neighbor)
		offset += 8
	}

	return mf.file.Sync()

}

func ReadSegmentFromDisk(id uint64, chunkSize int64, mf *MappedFile, vectorLenSize int64, neighborLenSize int64) (DiskSegment, error) {
	offset := int64(id) * chunkSize

	if offset+chunkSize > mf.size {
		return DiskSegment{}, os.ErrInvalid
	}

	// Read ID
	segmentId := binary.LittleEndian.Uint64(mf.data[offset:])
	offset += idSize

	// Read vector length
	vectorLen := binary.LittleEndian.Uint32(mf.data[offset:])
	offset += vectorLenSize

	// Read vector
	vector := make([]float32, vectorLen)
	for i := range vector {
		bits := binary.LittleEndian.Uint32(mf.data[offset:])
		vector[i] = math.Float32frombits(bits)
		offset += 4
	}

	// Read neighbors length
	neighborLen := binary.LittleEndian.Uint32(mf.data[offset:])
	offset += neighborLenSize

	// Read neighbors
	neighbors := make([]uint64, neighborLen)
	for i := range neighbors {
		neighbors[i] = binary.LittleEndian.Uint64(mf.data[offset:])
		offset += 8
	}

	return DiskSegment{
		Id:        segmentId,
		Vector:    vector,
		Neighbors: neighbors,
	}, nil
}

// MappedFile structure remains the same
type MappedFile struct {
	data []byte
	size int64
	file *os.File
}

func NewMappedFile(filename string, size int64) (*MappedFile, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}

	data, err := syscall.Mmap(
		int(f.Fd()),
		0,
		int(size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &MappedFile{
		data: data,
		size: size,
		file: f,
	}, nil
}

func (mf *MappedFile) Write(offset int64, data []byte) error {
	if offset+int64(len(data)) > mf.size {
		return os.ErrInvalid
	}
	copy(mf.data[offset:], data)
	return mf.file.Sync()
}

func (mf *MappedFile) Read(offset int64, length int) ([]byte, error) {
	if offset+int64(length) > mf.size {
		return nil, os.ErrInvalid
	}
	result := make([]byte, length)
	copy(result, mf.data[offset:offset+int64(length)])
	return result, nil
}

func (mf *MappedFile) Close() error {
	if err := syscall.Munmap(mf.data); err != nil {
		return err
	}
	return mf.file.Close()
}

// Example usage with bulk operations
func (mf *MappedFile) WriteChunks(data []byte, chunkSize int) error {
	for offset := int64(0); offset < int64(len(data)); offset += int64(chunkSize) {
		end := offset + int64(chunkSize)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if err := mf.Write(offset, data[offset:end]); err != nil {
			return err
		}
	}
	return nil
}
