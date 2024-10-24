package diskann

import (
	"bytes"
	"encoding/binary"
	"os"
	"syscall"
)

func WriteSegmentsToDisk(segments []*VamanaSegment, chunkSize int64, mf *MappedFile) error {

	for i := range segments {
		var neighbors []uint64
		for _, neighbor := range segments[i].neighbors {
			neighbors = append(neighbors, neighbor.id)
		}
		disk_segment := DiskSegment{id: segments[i].id, vector: segments[i].vector, neighbors: neighbors}

		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, disk_segment)

		if err != nil {
			return err
		}

		data := buf.Bytes()

		err = mf.Write(int64(i)*chunkSize, data)

	}

	return nil

}

func ReadSegmentFromDisk(id uint64, chunkSize int64, mf *MappedFile) (DiskSegment, error) {

	byteSegment, err := mf.Read(int64(id)*chunkSize, int(chunkSize))

	if err != nil {
		return DiskSegment{}, err
	}

	var disk_segment DiskSegment

	buf := bytes.NewBuffer(byteSegment)

	err = binary.Read(buf, binary.LittleEndian, &disk_segment)

	if err != nil {
		return DiskSegment{}, err
	}

	return disk_segment, nil

}

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
