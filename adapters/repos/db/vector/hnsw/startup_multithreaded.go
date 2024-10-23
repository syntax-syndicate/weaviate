package hnsw

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func (h *hnsw) restoreCommitLogMultiThreaded(fd *os.File, fileName string, state *DeserializationResult) (*DeserializationResult, error) {
	beforeAll := time.Now()
	defer func() {
		fmt.Printf("restoreCommitLogMultiThreaded took %s\n", time.Since(beforeAll))
	}()

	maxID, checkpoints, err := extractCheckpoints(fileName)
	if err != nil {
		return nil, err
	}

	if checkpoints[0] > 0 {
		r := io.NewSectionReader(fd, 0, int64(checkpoints[0]))
		var valid int
		state, valid, err = NewDeserializer(h.logger).Do(r, state, false)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// we need to check for both EOF or UnexpectedEOF, as we don't know where
				// the commit log got corrupted, a field ending that weset a longer
				// encoding for would return EOF, whereas a field read with binary.Read
				// with a fixed size would return UnexpectedEOF. From our perspective both
				// are unexpected.

				h.logger.WithField("action", "hnsw_load_commit_log_corruption").
					WithField("path", fileName).
					Error("write-ahead-log ended abruptly, some elements may not have been recovered")

				// we need to truncate the file to its valid length!
				if err := os.Truncate(fileName, int64(valid)); err != nil {
					return nil, errors.Wrapf(err, "truncate corrupt commit log %q", fileName)
				}
			} else {
				// only return an actual error on non-EOF errors, otherwise we'll end
				// up in a startup crashloop
				return nil, errors.Wrapf(err, "deserialize commit log %q", fileName)
			}
		}
	}

	fmt.Printf("loaded PQ data single threaded\n")

	newNodes, changed, err := growIndexToAccomodateNode(state.Nodes, maxID, h.logger)
	if err != nil {
		return nil, errors.Wrap(err, "grow index to accomodate node")
	}

	if changed {
		state.Nodes = newNodes
	}

	eg := enterrors.NewErrorGroupWrapper(h.logger)
	eg.SetLimit(2 * runtime.GOMAXPROCS(0))

	for i, cp := range checkpoints {
		if i == len(checkpoints)-1 {
			break
		}

		start := cp
		end := checkpoints[i+1]
		fmt.Printf("loading checkpoint %d to %d\n", start, end)

		eg.Go(func() error {
			r := io.NewSectionReader(fd, int64(start), int64(end-start))
			var valid int
			state, valid, err = NewDeserializer(h.logger).Do(r, state, false)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					// we need to check for both EOF or UnexpectedEOF, as we don't know where
					// the commit log got corrupted, a field ending that weset a longer
					// encoding for would return EOF, whereas a field read with binary.Read
					// with a fixed size would return UnexpectedEOF. From our perspective both
					// are unexpected.

					h.logger.WithField("action", "hnsw_load_commit_log_corruption").
						WithField("path", fileName).
						Error("write-ahead-log ended abruptly, some elements may not have been recovered")

					// TODO: This will break. We can't handle the error from multiple
					// threads when they're all reading from the same file

					// we need to truncate the file to its valid length!
					if err := os.Truncate(fileName, int64(valid)); err != nil {
						return errors.Wrapf(err, "truncate corrupt commit log %q", fileName)
					}
				} else {
					// only return an actual error on non-EOF errors, otherwise we'll end
					// up in a startup crashloop
					return errors.Wrapf(err, "deserialize commit log %q", fileName)
				}
			}

			return nil
		})

	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	fmt.Printf("read the remainder of the commit log to the end\n")
	fd.Seek(int64(checkpoints[len(checkpoints)-1]), io.SeekStart)

	var valid int
	state, valid, err = NewDeserializer(h.logger).Do(fd, state, false)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// we need to check for both EOF or UnexpectedEOF, as we don't know where
			// the commit log got corrupted, a field ending that weset a longer
			// encoding for would return EOF, whereas a field read with binary.Read
			// with a fixed size would return UnexpectedEOF. From our perspective both
			// are unexpected.

			h.logger.WithField("action", "hnsw_load_commit_log_corruption").
				WithField("path", fileName).
				Error("write-ahead-log ended abruptly, some elements may not have been recovered")

			// TODO: This will break. We can't handle the error from multiple
			// threads when they're all reading from the same file

			// we need to truncate the file to its valid length!
			if err := os.Truncate(fileName, int64(valid)); err != nil {
				return nil, errors.Wrapf(err, "truncate corrupt commit log %q", fileName)
			}
		} else {
			// only return an actual error on non-EOF errors, otherwise we'll end
			// up in a startup crashloop
			return nil, errors.Wrapf(err, "deserialize commit log %q", fileName)
		}
	}

	return state, nil
}

func extractCheckpoints(fileName string) (maxID uint64, checkpoints []uint64, err error) {
	cpfn := checkpointFileNameFromCondensed(fileName)
	cpFile, err := os.Open(cpfn)
	if err != nil {
		return 0, nil, err
	}

	buf, err := io.ReadAll(cpFile)
	if err != nil {
		return 0, nil, err
	}

	if len(buf) < 18 {
		return 0, nil, fmt.Errorf("corrupted checkpoint file %q", cpfn)
	}

	checksum := binary.LittleEndian.Uint32(buf[0:4])
	actualChecksum := crc32.ChecksumIEEE(buf[4:])
	if checksum != actualChecksum {
		return 0, nil, fmt.Errorf("corrupted checkpoint file %q, checksum mismatch", cpfn)
	}

	maxID = binary.LittleEndian.Uint64(buf[4:12])
	checkpoints = make([]uint64, 0, len(buf[12:])/8)
	for i := 12; i < len(buf); i += 8 {
		checkpoints = append(checkpoints, binary.LittleEndian.Uint64(buf[i:i+8]))
	}

	return maxID, checkpoints, nil
}

func checkpointFileNameFromCondensed(in string) string {
	return strings.TrimSuffix(in, ".condensed") + ".checkpoints"
}

func hasCheckpoints(fileName string) bool {
	if !strings.HasSuffix(fileName, ".condensed") {
		// checkpoint files are written as part of condensing. If a file does not
		// have the .condensed suffix, it cannot have a checkpoint
		return false
	}

	_, err := os.Stat(checkpointFileNameFromCondensed(fileName))
	return err == nil
}
