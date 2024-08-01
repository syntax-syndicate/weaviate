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

package lsmkv

import (
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

func (m *Memtable) newRoaringSetRangeCursor() roaringsetrange.InnerCursor {
	m.RLock()
	defer m.RUnlock()

	// Since Nodes makes deep copy of memtable bitmaps,
	// no further memtable's locking in required on cursor's methods
	return roaringsetrange.NewMemtableCursor(m.roaringSetRange)
}

func (m *Memtable) newRoaringSetRangeReader() roaringsetrange.InnerReader {
	m.RLock()
	defer m.RUnlock()

	return roaringsetrange.NewMemtableReader(m.roaringSetRange)
}

func (m *Memtable) newRoaringSetRangeCursorBS() roaringsetrange.InnerCursorBS {
	m.RLock()
	defer m.RUnlock()

	// Since Nodes makes deep copy of memtable bitmaps,
	// no further memtable's locking in required on cursor's methods
	return roaringsetrange.NewMemtableCursorBS(m.roaringSetRange)
}

func (m *Memtable) newRoaringSetRangeReaderBS() roaringsetrange.InnerReaderBS {
	m.RLock()
	defer m.RUnlock()

	return roaringsetrange.NewMemtableReaderBS(m.roaringSetRange)
}
