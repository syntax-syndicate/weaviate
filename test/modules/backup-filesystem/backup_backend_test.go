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

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/moduletools"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

var override = []string{"", ""}

func Test_FileSystemBackend_Start(t *testing.T) {
	overrides := [][]string{
		{"", ""},
		{"testbucketoverride", "testBucketPathOverride"},
	}

	override = overrides[0]
	FilesystemBackend_Backup(t)
	time.Sleep(5 * time.Second)

	override = overrides[1]
	FilesystemBackend_Backup(t)
}

func FilesystemBackend_Backup(t *testing.T) {
	t.Run("store backup meta", moduleLevelStoreBackupMeta)
	t.Run("copy objects", moduleLevelCopyObjects)
	t.Run("copy files", moduleLevelCopyFiles)
}

func moduleLevelStoreBackupMeta(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	backupDir := t.TempDir()
	className := "BackupClass"
	backupID := "backup_id"
	metadataFilename := "backup.json"

	t.Setenv("BACKUP_FILESYSTEM_PATH", backupDir)

	t.Run("store backup meta in fs", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		sp := fakeStorageProvider{dataDir}
		params := moduletools.NewInitParams(sp, nil, config.Config{}, logger)

		fs := modstgfs.New()
		err := fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("access permissions", func(t *testing.T) {
			err := fs.Initialize(testCtx, backupID, override[0], override[1])
			assert.Nil(t, err)
		})

		t.Run("backup meta does not exist yet", func(t *testing.T) {
			meta, err := fs.GetObject(testCtx, backupID, metadataFilename, override[0], override[1])
			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.IsType(t, backup.ErrNotFound{}, err)
		})

		t.Run("put backup meta on backend", func(t *testing.T) {
			desc := &backup.BackupDescriptor{
				StartedAt:   time.Now(),
				CompletedAt: time.Time{},
				ID:          backupID,
				Classes: []backup.ClassDescriptor{
					{
						Name: className,
					},
				},
				Status:  string(backup.Started),
				Version: ubak.Version,
			}

			b, err := json.Marshal(desc)
			require.Nil(t, err)

			err = fs.PutObject(testCtx, backupID, metadataFilename, "", "", b)
			require.Nil(t, err)

			dest := fs.HomeDir(backupID, override[0], override[1])
			expected := fmt.Sprintf("%s/%s", backupDir, backupID)
			assert.Equal(t, expected, dest)
		})
	})
}

func moduleLevelCopyObjects(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	backupDir := t.TempDir()
	key := "moduleLevelCopyObjects"
	backupID := "backup_id"

	t.Setenv("BACKUP_FILESYSTEM_PATH", backupDir)

	t.Run("copy objects", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		sp := fakeStorageProvider{dataDir}
		params := moduletools.NewInitParams(sp, nil, config.Config{}, logger)

		fs := modstgfs.New()
		err := fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("put object to bucket", func(t *testing.T) {
			err := fs.PutObject(testCtx, backupID, key, "", "", []byte("hello"))
			assert.Nil(t, err)
		})

		t.Run("get object from bucket", func(t *testing.T) {
			meta, err := fs.GetObject(testCtx, backupID, key, override[0], override[1])
			assert.Nil(t, err)
			assert.Equal(t, []byte("hello"), meta)
		})
	})
}

func moduleLevelCopyFiles(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	backupDir := t.TempDir()
	key := "moduleLevelCopyFiles"
	backupID := "backup_id"

	t.Setenv("BACKUP_FILESYSTEM_PATH", backupDir)

	t.Run("copy files", func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		logger, _ := test.NewNullLogger()
		sp := fakeStorageProvider{dataDir}
		params := moduletools.NewInitParams(sp, nil, config.Config{}, logger)

		fs := modstgfs.New()
		err = fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("verify source data path", func(t *testing.T) {
			assert.Equal(t, dataDir, fs.SourceDataPath())
		})

		t.Run("fetch file from backend", func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			err := fs.WriteToFile(testCtx, backupID, key, destPath, override[0], override[1])
			require.Nil(t, err)

			contents, err := os.ReadFile(destPath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})
	})
}

type fakeStorageProvider struct {
	dataPath string
}

func (sp fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (sp fakeStorageProvider) DataPath() string {
	return sp.dataPath
}
