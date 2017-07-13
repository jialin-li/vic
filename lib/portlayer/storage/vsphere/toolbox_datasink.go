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
	"bytes"
	"io"
	"path"

	"github.com/vmware/govmomi/guest"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/archive"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/vsphere/vm"
)

// ToolboxDataSink implements the DataSource interface for mounted devices
type ToolboxDataSink struct {
	ID    string
	VM    *vm.VirtualMachine
	Clean func()
}

// Source returns the data source associated with the DataSource
func (t *ToolboxDataSink) Sink() interface{} {
	return t.VM
}

// Import writes `data` to the data source associated with this DataSource
func (t *ToolboxDataSink) Import(op trace.Operation, spec *archive.FilterSpec, data io.ReadCloser) error {
	defer trace.End(trace.Begin(""))
	defer data.Close()
	// set up file manager
	client := t.VM.Session.Client.Client
	filemgr, err := guest.NewOperationsManager(client, t.VM.Reference()).FileManager(op)
	if err != nil {
		op.Errorf("AUTH ERR: %s", err.Error())
		return err
	}

	// authenticate to client tether
	auth := types.NamePasswordAuthentication{
		Username: t.ID,
	}

	// Initiate the upload
	dstPath := path.Join("/", path.Join(spec.StripPath, spec.RebasePath))
	op.Debugf("path: %s", dstPath)
	var buf bytes.Buffer
	size, err := io.Copy(&buf, data)
	op.Debugf("Initiating upload: path: %s, size: %d", dstPath, size)
	guestTransferURL, err := filemgr.InitiateFileTransferToGuest(op, &auth, dstPath, &types.GuestPosixFileAttributes{}, size, true)
	if err != nil {
		op.Errorf(err.Error())
		return err
	}

	// Upload the data tar to guest tools
	url, err := ParseArchiveUrl(op, client, guestTransferURL, spec)
	if err != nil {
		op.Errorf(err.Error())
		return err
	}
	op.Debugf("Uploading: %v", url)
	params := soap.DefaultUpload
	params.ContentLength = size
	err = client.Upload(bytes.NewReader(buf.Bytes()), url, &params)
	if err != nil {
		op.Errorf(err.Error())
		return err
	}

	return nil
}

func (t *ToolboxDataSink) Close() error {
	t.Clean()

	return nil
}
