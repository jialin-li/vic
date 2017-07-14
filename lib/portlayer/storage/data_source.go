// Copyright 2017 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"errors"
	"io"
	"os"

	"github.com/vmware/vic/lib/archive"
	"github.com/vmware/vic/pkg/trace"
	"path/filepath"
)

// MountDataSource implements the DataSource interface for mounted devices
type MountDataSource struct {
	Path  *os.File
	Clean func()
}

// Source returns the data source associated with the DataSource
func (m *MountDataSource) Source() interface{} {
	return m.Path
}

// Export reads data from the associated data source and returns it as a tar archive
func (m *MountDataSource) Export(op trace.Operation, spec *archive.FilterSpec, data bool) (io.ReadCloser, error) {
	fi, err := m.Path.Stat()
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return nil, errors.New("path must be a directory")
	}

	// NOTE: this isn't actually diffing - it's just creating a tar. @jzt to explain why
	return archive.Diff(op, m.Path.Name(), "", spec, data)
}

// Export reads data from the associated data source and returns it as a tar archive
func (m *MountDataSource) Stat(op trace.Operation, spec *archive.FilterSpec) (*FileStat, error){
	defer m.Close()

	// retrieve relative path
	if len(spec.Inclusions) != 1 {
		op.Errorf("incorrect number of paths to stat --- ", len(spec.Inclusions))
		return nil, errors.New("Incorrect number of paths to stat")
	}

	var targetPath string
	for path := range spec.Inclusions {
		targetPath = path
	}

	filePath := filepath.Join(m.Path.Name(), targetPath)
	fileInfo, err := os.Lstat(filePath)
	if err != nil {
		op.Errorf("failed to stat file")
		return nil, err
	}

	var linkTarget string
	// check for symlink
	if fileInfo.Mode() & os.ModeSymlink != 0 {
		linkTarget, err = os.Readlink(filePath)
		if err != nil {
			return nil, err
		}
	}

	fileName := fileInfo.Name()
	// handle root directory special case: don't want to return mntdir name
	if targetPath == "/" && spec.RebasePath == "" {
		fileName = "/"
	}

	return &FileStat{linkTarget, uint32(fileInfo.Mode()), fileName, fileInfo.Size(), fileInfo.ModTime()}, nil
}

func (m *MountDataSource) Close() error {
	m.Path.Close()
	m.Clean()

	return nil
}
