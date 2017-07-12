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

package vsphere

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/vmware/govmomi/guest"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/archive"
	"github.com/vmware/vic/lib/portlayer/storage"
	"github.com/vmware/vic/pkg/errors"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/vsphere/vm"
)

// ToolboxDataSource implements the DataSource interface for mounted devices
type ToolboxDataSource struct {
	ID    string
	VM    *vm.VirtualMachine
	Clean func()
}

// Source returns the data source associated with the DataSource
func (t *ToolboxDataSource) Source() interface{} {
	return t.VM
}

// Export reads data from the associated data source and returns it as a tar archive
func (t *ToolboxDataSource) Export(op trace.Operation, spec *archive.FilterSpec, data bool) (io.ReadCloser, error) {
	defer trace.End(trace.Begin(""))

	// set up file manager
	client := t.VM.Session.Client.Client
	filemgr, err := guest.NewOperationsManager(client, t.VM.Reference()).FileManager(op)
	if err != nil {
		return nil, err
	}

	// authenticate client and parse container host/port
	auth := types.NamePasswordAuthentication{
		Username: t.ID,
	}

	_ = filemgr
	_ = auth

	return nil, errors.New("toolbox export is not yet implemented")
}

// Export reads data from the associated data source and returns it as a tar archive
func (t *ToolboxDataSource) Stat(op trace.Operation, spec *archive.FilterSpec) (*storage.FileStat, error) {
	defer trace.End(trace.Begin(""))

	// resolve import path
	paths := archive.ResolveImportPath(spec)
	if len(paths) != 1 {
		op.Errorf("incorrect number of paths to stat: %s. --- %d != 1", t.ID, len(paths))
		return nil, errors.Errorf("Incorrect number of paths to stat")
	}

	file, err := t.inspectFile(op, paths[0])
	if err != nil {
		return nil, err
	}

	var mode uint32
	switch types.GuestFileType(file.Type) {
	case types.GuestFileTypeDirectory:
		mode = uint32(os.ModeDir)
	case types.GuestFileTypeSymlink:
		mode = uint32(os.ModeSymlink)
	default:
		mode = uint32(os.FileMode(uint32(0600)))
	}

	return &storage.FileStat{
		LinkTarget: file.Attributes.GetGuestFileAttributes().SymlinkTarget,
		Mode:       mode,
		Name:       filepath.Base(file.Path),
		Size:       file.Size,
		ModTime:    *file.Attributes.GetGuestFileAttributes().ModificationTime}, nil
}

// helper function to find the file using guesttool
func (t *ToolboxDataSource) inspectFile(op trace.Operation, path string) (*types.GuestFileInfo, error) {
	// err returned if file does not exist on the host
	client := t.VM.Session.Client.Client
	filemgr, err := guest.NewOperationsManager(client, t.VM.Reference()).FileManager(op)
	if err != nil {
		return nil, err
	}

	auth := types.NamePasswordAuthentication{
		Username: t.ID,
	}

	// List the files on the container path. If the path represents a regular
	// file, it's file info are returned. If the path represents a directory,
	// ListFiles will return all the files in that directory. This means that if
	// the for loop does not return becuase the path does not exist in files.Filed.
	// Consequently, we can safely assume the target is a directory.
	var offset int32
	files, err := filemgr.ListFiles(op, &auth, path, offset, 0, "")
	if err != nil {
		return nil, errors.Errorf("file listing for container %s failed\n: %s", t.ID, err.Error())
	}

	for _, file := range files.Files {
		op.Debugf("Stats for file %s --- %s\n", path, file)
		if file.Path == filepath.Base(path) {
			return &file, nil
		}
	}

	time := time.Now()
	return &types.GuestFileInfo{
		Path: path,
		Type: string(types.GuestFileTypeDirectory),
		Size: int64(4096),
		Attributes: &types.GuestFileAttributes{
			ModificationTime: &time,
		},
	}, nil
}

func (t *ToolboxDataSource) Close() error {
	t.Clean()

	return nil
}
