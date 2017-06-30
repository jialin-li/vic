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
	"io"

	log "github.com/Sirupsen/logrus"
	"github.com/vmware/govmomi/guest"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/portlayer/exec"
	"github.com/vmware/vic/pkg/trace"
)

func FileTransferFromGuest(ctx context.Context, vc *exec.Container, path string) (io.Reader, error) {
	defer trace.End(trace.Begin(vc.Config.Name))

	// set up file manager
	client := vc.VIM25Reference()
	filemgr, err := guest.NewOperationsManager(client, vc.VMReference()).FileManager(ctx)
	if err != nil {
		log.Errorf("OPS MGR ERR: %#v", err)
		return nil, err
	}
	auth := types.NamePasswordAuthentication{
		Username: vc.ExecConfig.ID,
	}

	// authenticate client and parse container host/port
	guestInfo, err := filemgr.InitiateFileTransferFromGuest(ctx, &auth, path)
	if err != nil {
		log.Errorf("INIT ERR: %#v --- %#v --- %#v --- %#v", err, ctx, &auth, path)
		return nil, err
	}

	url, err := client.ParseURL(guestInfo.Url)
	if err != nil {
		log.Errorf("PARSE ERR: %#v", err)
		return nil, err
	}

	rc, _, err := client.Download(url, &soap.DefaultDownload)
	defer rc.Close()

	if err != nil {
		log.Errorf("DOWNLOAD ERR: %#v", err)
		return nil, err
	}
	log.Debugf("DOWNLOADED %#v", rc)
	return rc, nil
}

func FileTransferToGuest(ctx context.Context, vc *exec.Container, path string, reader io.Reader) error {
	defer trace.End(trace.Begin(vc.Config.Name))

	// set up file manager
	client := vc.VIM25Reference()
	filemgr, err := guest.NewOperationsManager(client, vc.VMReference()).FileManager(ctx)
	if err != nil {
		log.Errorf("OPS MGR ERR: %#v", err)
		return err
	}
	auth := types.NamePasswordAuthentication{
		Username: vc.ExecConfig.ID,
	}

	// authenticate client and parse container host/port
	guestTransferURL, err := filemgr.InitiateFileTransferToGuest(ctx, &auth, path, &types.GuestPosixFileAttributes{}, 0, true)
	if err != nil {
		log.Errorf("INIT ERR: %#v", err)
		return err
	}

	url, err := client.ParseURL(guestTransferURL)
	if err != nil {
		log.Errorf("PARSE ERR: %#v", err)
		return err
	}

	err = client.Upload(reader, url, &soap.DefaultUpload)
	if err != nil {
		log.Errorf("UPLOAD ERR: %#v", err)
		return err
	}
	log.Debugf("UPLOADED %#v", reader)
	return nil
}
