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
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/vmware/govmomi/guest"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/archive"
	"github.com/vmware/vic/lib/portlayer/storage"
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
		op.Errorf("AUTH ERR: %s", err.Error())
		return nil, err
	}

	auth := types.NamePasswordAuthentication{
		Username: t.ID,
	}

	var readers []io.Reader
	// filterspec inclusion paths should be absolute to the container root
	for path := range spec.Inclusions {
		op.Debugf("path: %s", path)
		// authenticate client and parse container host/port.
		guestInfo, err := filemgr.InitiateFileTransferFromGuest(op, &auth, path)
		if err != nil {
			op.Errorf("INIT ERR: %s", err.Error())
			return nil, err
		}

		url, err := ParseArchiveUrl(op, client, guestInfo.Url, spec)
		if err != nil {
			op.Errorf("PARSE ERR: %s", err.Error())
			return nil, err
		}

		// download from guest. if download is a file, create a tar out of it.
		// guest tools will not tar up single files.
		op.Debugf("Downloading: %v --- %s from %d", url, path, t.ID[:8])
		params := soap.DefaultDownload
		rc, _, err := client.Download(url, &params)
		if err != nil {
			op.Errorf("DOWNLOAD ERR: %s", err.Error())
			return nil, err
		}

		readers = append(readers, rc)

	}

	return ioutil.NopCloser(io.MultiReader(readers...)), nil
}

// Export reads data from the associated data source and returns it as a tar archive
func (t *ToolboxDataSource) Stat(op trace.Operation, spec *archive.FilterSpec) (*storage.FileStat, error) {
	defer trace.End(trace.Begin(""))

	statTar, err := t.Export(op, spec, false)
	if err != nil {
		op.Errorf(err.Error())
		return nil, err
	}
	defer statTar.Close()

	tarReader := tar.NewReader(statTar)
	header, err := tarReader.Next()
	if err != nil || header == nil {
		return nil, fmt.Errorf("unable to export stat for: %s -- %#v", t.ID, spec)
	}

	stat := &storage.FileStat{
		Mode:    uint32(header.Mode),
		Name:    header.Name,
		Size:    header.Size,
		ModTime: header.ModTime,
	}

	if header.Typeflag == tar.TypeSymlink {
		stat.LinkTarget = header.Linkname
	}

	return stat, nil
}

func (t *ToolboxDataSource) Close() error {
	t.Clean()

	return nil
}
