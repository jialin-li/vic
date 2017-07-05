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

// StatPath will use Guest Tools to stat a given path in the container

package compute

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"io/ioutil"

	log "github.com/Sirupsen/logrus"

	"github.com/vmware/govmomi/guest"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/portlayer/exec"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/errors"
	"github.com/vmware/vic/pkg/vsphere/disk"
)

// TODO: still need to figure this part out
type FileStat struct {
	LinkTarget string
	Mode       uint32
	Name       string
	Size       int64
}

// interface for offline container statpath
type ContainerStatPath interface {
	// deviceId, filepath
	StatPath(op trace.Operation, deviceId string, target string) (*FileStat, error)
}

func StatPath(ctx context.Context, vc *exec.Container, path string) (*types.GuestFileInfo, error) {
	defer trace.End(trace.Begin(vc.Config.Name))

	// below implementation of searching one directory up does not work at root, but we can assume root is a directory
	if path == "/" {
		return &types.GuestFileInfo{
			Path: "/",
			Type: string(types.GuestFileTypeDirectory),
			Size: int64(4096),
		}, nil
	}

	filemgr, err := guest.NewOperationsManager(vc.VIM25Reference(), vc.VMReference()).FileManager(ctx)
	if err != nil {
		return nil, err
	}

	auth := types.NamePasswordAuthentication{
		Username: vc.ExecConfig.ID,
	}

	// list files one directory up form the target path to include directories.
	pathBasename := filepath.Base(path)
	for _, path := range []string{path, strings.Replace(path, pathBasename, "", -1)} {
		var offset int32
		files, err := filemgr.ListFiles(ctx, &auth, path, offset, 0, "")
		if err != nil {
			return nil, fmt.Errorf("file listing for container %s failed\n: %s", vc.ExecConfig.ID, err.Error())
		}

		for _, file := range files.Files {
			log.Debugf("Stats for file %s --- %s\n", path, file)
			if file.Path == pathBasename {
				return &file, nil
			}
		}
	}

	return nil, fmt.Errorf("file %s not found on container %s", path, vc.ExecConfig.ID)
}

// OfflineStatPath creates a tmp directory to mount the disk and then inspect the file stat
func OfflineStatPath(op trace.Operation, dsk *disk.VirtualDisk, target string) (*FileStat, error) {
	// tmp dir to mount the disk
	dir, err := ioutil.TempDir("", "mntfs")
	if err != nil {
		err = errors.Errorf("Failed to create temp dir %s ", err.Error())
		return nil, err
	}

	defer func() {
		e1 := os.RemoveAll(dir)
		if e1 != nil {
			op.Errorf("Failed to remove tempDir: %s", e1)
			if err == nil {
				err = e1
			}
		}
	}()

	err = dsk.Mount(dir, nil)
	if err != nil {
		op.Debugf("Failed to mount the disk")
		return nil, errors.Errorf("Failed to mount: %s ", err.Error())
	}

	defer func() {
		e1 := dsk.Unmount()
		if e1 != nil {
			op.Debugf("Failed to unmount device: %s", err)
			err = errors.Errorf("Failed to unmount device, error is %s ", err.Error())
		}
	}()

	return inspectFileStat(filepath.Join(dir, target))
}

// InspectFileStat runs lstat on the target
func inspectFileStat(target string) (*FileStat, error) {
	fileInfo, err := os.Lstat(target)
	if err != nil {
		return nil, errors.Errorf("error returned from %s, target %s", err.Error(), target)
	}

	var linkTarget string
	// check for symlink
	if fileInfo.Mode() & os.ModeSymlink != 0 {
		linkTarget, err = os.Readlink(target)
		if err != nil {
			return nil, err
		}
	}

	return &FileStat{linkTarget, uint32(fileInfo.Mode()), fileInfo.Name(), fileInfo.Size()}, nil
}

//func Test(path string) string {
//	mnt, err := os.Open(path)
//	if err != nil {
//		return err.Error()
//	}
//
//	info, err := mnt.Readdir(-1)
//	if err != nil {
//		return err.Error()
//	}
//
//	var result string
//	for _, file := range info {
//		result = result + file.Name()
//	}
//	return result
//}