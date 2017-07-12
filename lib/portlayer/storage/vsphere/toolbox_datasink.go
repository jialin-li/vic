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
	"errors"
	"io"

	"github.com/vmware/govmomi/guest"
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

	// set up file manager
	client := t.VM.Session.Client.Client
	filemgr, err := guest.NewOperationsManager(client, t.VM.Reference()).FileManager(op)
	if err != nil {
		return err
	}

	// authenticate client and parse container host/port
	auth := types.NamePasswordAuthentication{
		Username: t.ID,
	}

	_ = filemgr
	_ = auth

	return errors.New("toolbox import is not yet implemented")
}

func (t *ToolboxDataSink) Close() error {
	t.Clean()

	return nil
}

// // GuestTransfer provides a mechanism to serially download multiple files from
// // Guest Tools. This should be replaced with a portlayer-wide, container-specific
// // Download Manager, as VIX calls cannot be made in parallel on a vm.
// type GuestTransfer struct {
// 	GuestTransferURL string
// 	Reader           io.Reader
// 	Size             int64
// }

// func FileTransferToGuest(op trace.Operation, vc *exec.Container, fs archive.FilterSpec, reader io.ReadCloser) error {
// 	defer trace.End(trace.Begin(""))

// 	// set up file manager
// 	client := vc.VIM25Reference()
// 	filemgr, err := guest.NewOperationsManager(client, vc.VMReference()).FileManager(op)
// 	if err != nil {
// 		return err
// 	}

// 	// authenticate client and parse container host/port
// 	auth := types.NamePasswordAuthentication{
// 		Username: vc.ExecConfig.ID,
// 	}

// 	tarReader := tar.NewReader(reader)
// 	defer reader.Close()

// 	var uploadLock = &sync.Mutex{}
// 	for {
// 		header, err := tarReader.Next()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			return err
// 		}
// 		if header == nil {
// 			continue
// 		}
// 		gZipOut, gZipIn := io.Pipe()
// 		go func() {
// 			gZipWriter := gzip.NewWriter(gZipIn)
// 			tarWriter := tar.NewWriter(gZipWriter)
// 			defer gZipIn.Close()
// 			defer gZipWriter.Close()
// 			defer tarWriter.Close()

// 			if err = tarWriter.WriteHeader(header); err != nil {
// 				op.Errorf(err.Error())
// 				gZipIn.CloseWithError(err)
// 				return
// 			}

// 			if header.Typeflag == tar.TypeReg {
// 				if _, err = io.Copy(tarWriter, tarReader); err != nil {
// 					op.Errorf(err.Error())
// 					gZipIn.CloseWithError(err)
// 					return
// 				}
// 			}

// 		}()
// 		var byteWrapper []byte
// 		var size int64
// 		uploadLock.Lock()
// 		go func() {
// 			defer uploadLock.Unlock()

// 			byteWrapper, err = ioutil.ReadAll(gZipOut)
// 			if err != nil {
// 				op.Errorf(err.Error())
// 				gZipOut.CloseWithError(err)
// 				return
// 			}
// 			size = int64(len(byteWrapper))
// 		}()
// 		uploadLock.Lock()
// 		if size == 0 {
// 			return fmt.Errorf("upload size cannot be 0")
// 		}

// 		path := "/" + strings.TrimSuffix(strings.TrimPrefix(fs.StripPath, "/"), "/") + "/"
// 		op.Debugf("Initiating upload: path: %s to %s, size: %d on %s", header.Name, path, size, vc.ExecConfig.ID)
// 		guestTransferURL, err := filemgr.InitiateFileTransferToGuest(op, &auth, path, &types.GuestPosixFileAttributes{}, size, true)
// 		if err != nil {
// 			op.Errorf(err.Error())
// 			return err
// 		}
// 		url, err := client.ParseURL(guestTransferURL)
// 		if err != nil {
// 			op.Errorf(err.Error())
// 			return err
// 		}
// 		// upload tar archive to url
// 		op.Debugf("Uploading: %v --- %s", url, vc.ExecConfig.ID)
// 		params := soap.DefaultUpload
// 		params.ContentLength = size
// 		err = client.Upload(bytes.NewReader(byteWrapper), url, &params)
// 		if err != nil {
// 			op.Errorf(err.Error())
// 			return err
// 		}
// 		uploadLock.Unlock()
// 	}
// 	return nil
// }
