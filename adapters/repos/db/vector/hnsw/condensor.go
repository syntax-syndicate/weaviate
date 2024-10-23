//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/commitlog"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

type MemoryCondensor struct {
	newLogFile *os.File
	newLog     *bufWriter
	logger     logrus.FieldLogger
}

func (c *MemoryCondensor) Do(fileName string) error {
	c.logger.WithField("action", "hnsw_condensing").Infof("start hnsw condensing")
	defer c.logger.WithField("action", "hnsw_condensing_complete").Infof("completed hnsw condensing")

	fd, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "open commit log to be condensed")
	}
	defer fd.Close()
	fdBuf := bufio.NewReaderSize(fd, 256*1024)

	res, _, err := NewDeserializer(c.logger).Do(fdBuf, nil, true)
	if err != nil {
		return errors.Wrap(err, "read commit log to be condensed")
	}

	newLogFile, err := os.OpenFile(fmt.Sprintf("%s.condensed", fileName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrap(err, "open new commit log file for writing")
	}

	c.newLogFile = newLogFile

	offset := 0
	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)

	if res.Compressed {
		if n, err := c.AddPQ(res.PQData); err != nil {
			return fmt.Errorf("write pq data: %w", err)
		} else {
			offset += n
		}
	}

	fmt.Printf("begin offset finished AddNode %d\n", offset)

	chunkSize := 100_000

	nodesWritten := 0
	maxNodeId := uint64(0)
	var offsets []int
	for _, node := range res.Nodes {
		if node == nil {
			// nil nodes occur when we've grown, but not inserted anything yet
			continue
		}

		if nodesWritten%chunkSize == 0 {
			offsets = append(offsets, offset)
		}

		if node.id > maxNodeId {
			maxNodeId = node.id
		}

		if node.level > 0 {
			// nodes are implicitly added when they are first linked, if the level is
			// not zero we know this node was new. If the level is zero it doesn't
			// matter if it gets added explicitly or implicitly
			if err := c.AddNode(node); err != nil {
				return errors.Wrapf(err, "write node %d to commit log", node.id)
			} else {
				offset += commitlog.AddNodeSize
			}
		}

		for level, links := range node.connections {
			if res.ReplaceLinks(node.id, uint16(level)) {
				if n, err := c.SetLinksAtLevel(node.id, level, links); err != nil {
					return errors.Wrapf(err,
						"write links for node %d at level %d to commit log", node.id, level)
				} else {
					offset += n
				}
			} else {
				if n, err := c.AddLinksAtLevel(node.id, uint16(level), links); err != nil {
					return errors.Wrapf(err,
						"write links for node %d at level %d to commit log", node.id, level)
				} else {
					offset += n
				}
			}
		}

		nodesWritten++
	}

	offsets = append(offsets, offset)
	fmt.Printf("final offset finished AddNode %d\n", offset)

	if res.EntrypointChanged {
		if err := c.SetEntryPointWithMaxLayer(res.Entrypoint,
			int(res.Level)); err != nil {
			return errors.Wrap(err, "write entrypoint to commit log")
		}
	}

	for ts := range res.Tombstones {
		// If the tombstone was later removed, consolidate the two operations into a noop
		if _, ok := res.TombstonesDeleted[ts]; ok {
			continue
		}

		if err := c.AddTombstone(ts); err != nil {
			return errors.Wrapf(err,
				"write tombstone for node %d to commit log", ts)
		}
	}

	for rmts := range res.TombstonesDeleted {
		// If the tombstone was added previously, consolidate the two operations into a noop
		if _, ok := res.Tombstones[rmts]; ok {
			continue
		}

		if err := c.RemoveTombstone(rmts); err != nil {
			return errors.Wrapf(err,
				"write removed tombstone for node %d to commit log", rmts)
		}
	}

	for nodesDeleted := range res.NodesDeleted {
		if err := c.DeleteNode(nodesDeleted); err != nil {
			return errors.Wrapf(err,
				"write deleted node %d to commit log", nodesDeleted)
		}
	}

	if err := c.newLog.Flush(); err != nil {
		return errors.Wrap(err, "close new commit log")
	}

	if err := c.newLogFile.Close(); err != nil {
		return errors.Wrap(err, "close new commit log")
	}

	if err := c.writeCheckpoints(fileName, maxNodeId, offsets); err != nil {
		return errors.Wrap(err, "write checkpoints")
	}

	if err := os.Remove(fileName); err != nil {
		return errors.Wrap(err, "cleanup old (uncondensed) commit log")
	}

	return nil
}

func (c *MemoryCondensor) writeUint64(w *bufWriter, in uint64) error {
	toWrite := make([]byte, 8)
	binary.LittleEndian.PutUint64(toWrite[0:8], in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor) writeUint16(w *bufWriter, in uint16) error {
	toWrite := make([]byte, 2)
	binary.LittleEndian.PutUint16(toWrite[0:2], in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor) writeCommitType(w *bufWriter, in HnswCommitType) error {
	toWrite := make([]byte, 1)
	toWrite[0] = byte(in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor) writeUint64Slice(w *bufWriter, in []uint64) error {
	for _, v := range in {
		err := c.writeUint64(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddNode adds an empty node
func (c *MemoryCondensor) AddNode(node *vertex) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, AddNode))
	ec.Add(c.writeUint64(c.newLog, node.id))
	ec.Add(c.writeUint16(c.newLog, uint16(node.level)))

	return ec.ToError()
}

func (c *MemoryCondensor) DeleteNode(id uint64) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, DeleteNode))
	ec.Add(c.writeUint64(c.newLog, id))

	return ec.ToError()
}

func (c *MemoryCondensor) SetLinksAtLevel(nodeid uint64, level int, targets []uint64) (int, error) {
	n := 0
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, ReplaceLinksAtLevel))
	n += 1

	ec.Add(c.writeUint64(c.newLog, nodeid))
	n += 8

	ec.Add(c.writeUint16(c.newLog, uint16(level)))
	n += 2

	targetLength := len(targets)
	if targetLength > math.MaxUint16 {
		// TODO: investigate why we get such massive connections
		targetLength = math.MaxUint16
		c.logger.WithField("action", "condense_commit_log").
			WithField("original_length", len(targets)).
			WithField("maximum_length", targetLength).
			Warning("condensor length of connections would overflow uint16, cutting off")
	}
	ec.Add(c.writeUint16(c.newLog, uint16(targetLength)))
	n += 2

	ec.Add(c.writeUint64Slice(c.newLog, targets[:targetLength]))
	n += targetLength * 8

	return n, ec.ToError()
}

func (c *MemoryCondensor) AddLinksAtLevel(nodeid uint64, level uint16, targets []uint64) (int, error) {
	toWrite := make([]byte, 13+len(targets)*8)
	toWrite[0] = byte(AddLinksAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], nodeid)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	binary.LittleEndian.PutUint16(toWrite[11:13], uint16(len(targets)))
	for i, target := range targets {
		offsetStart := 13 + i*8
		offsetEnd := offsetStart + 8
		binary.LittleEndian.PutUint64(toWrite[offsetStart:offsetEnd], target)
	}
	return c.newLog.Write(toWrite)
}

func (c *MemoryCondensor) AddLinkAtLevel(nodeid uint64, level uint16, target uint64) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, AddLinkAtLevel))
	ec.Add(c.writeUint64(c.newLog, nodeid))
	ec.Add(c.writeUint16(c.newLog, uint16(level)))
	ec.Add(c.writeUint64(c.newLog, target))

	return ec.ToError()
}

func (c *MemoryCondensor) SetEntryPointWithMaxLayer(id uint64, level int) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, SetEntryPointMaxLevel))
	ec.Add(c.writeUint64(c.newLog, id))
	ec.Add(c.writeUint16(c.newLog, uint16(level)))

	return ec.ToError()
}

func (c *MemoryCondensor) AddTombstone(nodeid uint64) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, AddTombstone))
	ec.Add(c.writeUint64(c.newLog, nodeid))

	return ec.ToError()
}

func (c *MemoryCondensor) RemoveTombstone(nodeid uint64) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, RemoveTombstone))
	ec.Add(c.writeUint64(c.newLog, nodeid))

	return ec.ToError()
}

func (c *MemoryCondensor) AddPQ(data compressionhelpers.PQData) (int, error) {
	toWrite := make([]byte, 10)
	toWrite[0] = byte(AddPQ)
	binary.LittleEndian.PutUint16(toWrite[1:3], data.Dimensions)
	toWrite[3] = byte(data.EncoderType)
	binary.LittleEndian.PutUint16(toWrite[4:6], data.Ks)
	binary.LittleEndian.PutUint16(toWrite[6:8], data.M)
	toWrite[8] = data.EncoderDistribution
	if data.UseBitsEncoding {
		toWrite[9] = 1
	} else {
		toWrite[9] = 0
	}

	for _, encoder := range data.Encoders {
		toWrite = append(toWrite, encoder.ExposeDataForRestore()...)
	}
	return c.newLog.Write(toWrite)
}

func NewMemoryCondensor(logger logrus.FieldLogger) *MemoryCondensor {
	return &MemoryCondensor{logger: logger}
}

func (c *MemoryCondensor) writeCheckpoints(
	fileName string, maxNodeId uint64, checkpoints []int,
) error {
	checkpointFile, err := os.OpenFile(fmt.Sprintf("%s.checkpoints", fileName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrap(err, "open new checkpoint file for writing")
	}
	defer checkpointFile.Close()

	// 0-4: checksum
	// 4-12: max node id
	// 12+: checkpoints (8 bytes each)
	buffer := make([]byte, 4+8+len(checkpoints)*8)
	offset := 4
	binary.LittleEndian.PutUint64(buffer[offset:offset+8], maxNodeId)
	offset += 8

	for _, cp := range checkpoints {
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], uint64(cp))
		offset += 8
	}

	checksum := crc32.ChecksumIEEE(buffer[4:])
	binary.LittleEndian.PutUint32(buffer[0:4], checksum)

	_, err = checkpointFile.Write(buffer)
	return err
}
