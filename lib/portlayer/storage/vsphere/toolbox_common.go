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
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/vic/lib/archive"
	"github.com/vmware/vic/pkg/trace"
)

// Parse Archive does something.
func ParseArchiveUrl(op trace.Operation, client *vim25.Client, url string, fs *archive.FilterSpec) (*url.URL, error) {
	u, err := client.ParseURL(url)
	if err != nil {
		return nil, err
	}
	encodedSpec, err := archive.EncodeFilterSpec(op, fs)
	if err != nil {
		return nil, err
	}
	query := u.Query()
	query.Set("filterspec", *encodedSpec)
	u.RawQuery = query.Encode()

	op.Debugf("adding filterspec %#v to url %#v", fs, url)
	return u, nil
}

type ToolboxOverrideArchiveHandler struct {
	Read  func(*url.URL, *tar.Reader) error
	Write func(*url.URL, *tar.Writer) error
}

// archiveWrite writes the contents of the given source directory to the given tar.Writer.
func (ts *ToolboxOverrideArchiveHandler) archiveWrite(u *url.URL, tw *tar.Writer) error {
	dir := filepath.Dir(u.Path)
	var op trace.Operation
	return filepath.Walk(u.Path, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return filepath.SkipDir
		}

		var spec *archive.FilterSpec
		if spec.Excludes(op, file) {
			return nil
		}

		name := strings.TrimPrefix(file, dir)[1:]

		header, _ := tar.FileInfoHeader(fi, name)

		header.Name = name

		if header.Typeflag == tar.TypeDir {
			header.Name += "/"
		}

		var f *os.File

		if header.Typeflag == tar.TypeReg && fi.Size() != 0 {
			f, err = os.Open(file)
			if err != nil {
				if os.IsPermission(err) {
					return nil
				}
				return err
			}
		}

		_ = tw.WriteHeader(header)

		if f != nil {
			_, err = io.Copy(tw, f)
			_ = f.Close()
		}

		return err
	})
}

// archiveRead writes the contents of the given tar.Reader to the given directory.
func (ts *ToolboxOverrideArchiveHandler) archiveRead(u *url.URL, tr *tar.Reader) error {
	for {
		header, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		name := filepath.Join(u.Path, header.Name)
		mode := os.FileMode(header.Mode)

		switch header.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(name, mode)
		case tar.TypeReg:
			_ = os.MkdirAll(filepath.Dir(name), 0755)

			var f *os.File

			f, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
			if err == nil {
				_, cerr := io.Copy(f, tr)
				err = f.Close()
				if cerr != nil {
					err = cerr
				}
			}
		case tar.TypeSymlink:
			err = os.Symlink(header.Linkname, name)
		}

		// TODO: Uid/Gid may not be meaningful here without some mapping.
		// The other option to consider would be making use of the guest auth user ID.
		// os.Lchown(name, header.Uid, header.Gid)

		if err != nil {
			return err
		}
	}
}

func unZipAndExcldueTar(op trace.Operation, reader io.ReadCloser, spec *archive.FilterSpec, includePath string, data bool) io.ReadCloser {
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

// func upload() {
// // set up file manager
// client := t.VM.Session.Client.Client
// filemgr, err := guest.NewOperationsManager(client, t.VM.Reference()).FileManager(op)
// if err != nil {
// 	op.Errorf("AUTH ERR: %s", err.Error())
// 	return err
// }

// // authenticate to client tether
// auth := types.NamePasswordAuthentication{
// 	Username: t.ID,
// }

// tarReader := tar.NewReader(data)
// defer data.Close()

// uploadLock := &sync.Mutex{}
// for {
// 	header, err := tarReader.Next()
// 	if err == io.EOF {
// 		break
// 	}

// 	if err != nil {
// 		op.Errorf("TAR ERR: %s", err.Error())
// 		return err
// 	}

// 	dstPath := path.Join("/", path.Join(spec.RebasePath, strings.TrimPrefix(spec.StripPath, header.Name)))
// 	op.Debugf("path: %s", dstPath)

// 	op.Debugf("Calling upload for asset: %v --- %s", *header, dstPath)

// 	gZipOut, gZipIn := io.Pipe()
// 	go func() {
// 		gZipWriter := gzip.NewWriter(gZipIn)
// 		tarWriter := tar.NewWriter(gZipWriter)
// 		defer gZipIn.Close()
// 		defer gZipWriter.Close()
// 		defer tarWriter.Close()

// 		if err = tarWriter.WriteHeader(header); err != nil {
// 			op.Errorf(err.Error())
// 			gZipIn.CloseWithError(err)
// 			return
// 		}

// 		if header.Typeflag == tar.TypeReg {
// 			if _, err = io.Copy(tarWriter, tarReader); err != nil {
// 				op.Errorf(err.Error())
// 				gZipIn.CloseWithError(err)
// 				return
// 			}
// 		}

// 	}()

// 	var byteWrapper []byte
// 	var size int64
// 	uploadLock.Lock()
// 	go func() {
// 		defer uploadLock.Unlock()

// 		byteWrapper, err = ioutil.ReadAll(gZipOut)
// 		if err != nil {
// 			op.Errorf(err.Error())
// 			gZipOut.CloseWithError(err)
// 			return
// 		}
// 		size = int64(len(byteWrapper))
// 	}()

// 	uploadLock.Lock()
// 	if size == 0 {
// 		return fmt.Errorf("upload size cannot be 0")
// 	}

// 	op.Debugf("Initiating upload: path: %s to %s, size: %d", header.Name, dstPath, size)
// 	guestTransferURL, err := filemgr.InitiateFileTransferToGuest(op, &auth, dstPath, &types.GuestPosixFileAttributes{}, size, true)
// 	if err != nil {
// 		op.Errorf(err.Error())
// 		return err
// 	}
// 	url, err := ParseArchiveUrl(op, client, guestTransferURL, spec)
// 	if err != nil {
// 		op.Errorf(err.Error())
// 		return err
// 	}
// 	// upload tar archive to url
// 	op.Debugf("Uploading: %v", url)
// 	params := soap.DefaultUpload
// 	params.ContentLength = size
// 	err = client.Upload(bytes.NewReader(byteWrapper), url, &params)
// 	if err != nil {
// 		op.Errorf(err.Error())
// 		return err
// 	}
// 	uploadLock.Unlock()

// }
// return nil
// }
