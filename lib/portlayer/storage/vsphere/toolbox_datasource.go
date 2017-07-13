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
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"path/filepath"

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
	defer trace.End(trace.Begin("toolbox export"))

	// set up file manager
	client := t.VM.Session.Client.Client
	filemgr, err := guest.NewOperationsManager(client, t.VM.Reference()).FileManager(op)
	if err != nil {
		op.Debugf("Failed to create new op manager ")
		return nil, err
	}

	auth := types.NamePasswordAuthentication{
		Username: t.ID,
	}

	var readers []io.Reader
	for path := range spec.Inclusions {
		op.Debugf("path: %s", filepath.Join(spec.RebasePath, path))
		// authenticate client and parse container host/port.
		guestInfo, err := filemgr.InitiateFileTransferFromGuest(op, &auth, filepath.Join(spec.RebasePath, path))
		if err != nil {
			op.Debugf("Failed to initiate file transfer ")
			return nil, err
		}

		url, err := client.ParseURL(guestInfo.Url)
		if err != nil {
			op.Debugf("Failed to parse url ")
			return nil, err
		}

		// download from guest. if download is a file, create a tar out of it.
		// guest tools will not tar up single files.
		op.Debugf("Downloading: %v --- %s from %d", url, path, t.ID[:8])
		params := soap.DefaultDownload
		rc, _, err := client.Download(url, &params)
		if err != nil {
			op.Debugf("Failed to download ")
			return nil, err
		}

		readers = append(readers, t.unZipAndExcldueTar(op, rc, spec, path, data))

	}

	return ioutil.NopCloser(io.MultiReader(readers...)), nil
}

// Export reads data from the associated data source and returns it as a tar archive
func (t *ToolboxDataSource) Stat(op trace.Operation, spec *archive.FilterSpec) (*storage.FileStat, error) {
	defer trace.End(trace.Begin(""))

	statTar, err := t.Export(op, spec, false)
	if err != nil {
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

//----------
// Utility Functions
//----------

func (t *ToolboxDataSource) unZipAndExcldueTar(op trace.Operation, reader io.ReadCloser, spec *archive.FilterSpec, includePath string, data bool) io.ReadCloser {
	// create a writer for gzip compressiona nd a tar archive
	tarOut, tarIn := io.Pipe()
	go func() {
		gZipReader, err := gzip.NewReader(reader)
		if err != nil {
			op.Errorf("Error in unziptar: %s", err.Error())
			tarIn.CloseWithError(err)
			return
		}
		tarReader := tar.NewReader(gZipReader)
		tarWriter := tar.NewWriter(tarIn)
		tarDiscardWriter := tar.NewWriter(ioutil.Discard)
		defer reader.Close()
		defer tarIn.Close()
		defer gZipReader.Close()
		defer tarDiscardWriter.Close()
		defer tarWriter.Close()

		// grab tar stream from guest tools. zip it up if there are no errors
		for {
			hdr, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				op.Errorf("Error in unziptar: %s", err.Error())
				tarIn.CloseWithError(err)
				return
			}

			writer := tarWriter
			path := path.Join(path.Dir(includePath), hdr.Name)
			if excluded := spec.Excludes(op, path); excluded {
				op.Debugf("Getting exclusion for: %s --- %t", path, excluded)
				writer = tarDiscardWriter
			}

			op.Debugf("read/write header: %#v", *hdr)
			if err = writer.WriteHeader(hdr); err != nil {
				op.Errorf("Error in unziptar: %s", err.Error())
				tarIn.CloseWithError(err)
				return
			}

			if !data {
				writer = tarDiscardWriter
			}

			op.Debugf("read/write body")
			if _, err := io.Copy(writer, tarReader); err != nil {
				op.Errorf("Error in unziptar: %s", err.Error())
				tarIn.CloseWithError(err)
				return
			}

		}

		op.Debugf("return")
	}()

	return tarOut
}

