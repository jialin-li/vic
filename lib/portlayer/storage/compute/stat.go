// Copyright 2016-2017 VMware, Inc. All Rights Reserved.
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
	"path/filepath"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/vmware/govmomi/guest"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/portlayer/exec"
	"github.com/vmware/vic/pkg/trace"
)

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
