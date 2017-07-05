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

package vsphere

import (
	"fmt"
	"net/url"
	"os"
	"io/ioutil"
	"path"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/config/executor"
	"github.com/vmware/vic/lib/portlayer/storage"
	"github.com/vmware/vic/lib/portlayer/storage/compute"
	"github.com/vmware/vic/lib/portlayer/util"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/vsphere/datastore"
	"github.com/vmware/vic/pkg/vsphere/disk"
	"github.com/vmware/vic/pkg/vsphere/session"
	"github.com/vmware/vic/pkg/errors"
)

const VolumesDir = "volumes"

// VolumeStore caches Volume references to volumes in the system.
type VolumeStore struct {
	// helper to the backend
	ds *datastore.Helper

	// wraps our vmdks and filesystem primitives.
	dm *disk.Manager

	// Service url to this VolumeStore
	SelfLink *url.URL
}

func NewVolumeStore(op trace.Operation, storeName string, s *session.Session, ds *datastore.Helper) (*VolumeStore, error) {
	// Create the volume dir if it doesn't already exist
	if _, err := ds.Mkdir(op, true, VolumesDir); err != nil && !os.IsExist(err) {
		return nil, err
	}

	dm, err := disk.NewDiskManager(op, s)
	if err != nil {
		return nil, err
	}

	if DetachAll {
		if err = dm.DetachAll(op); err != nil {
			return nil, err
		}
	}

	u, err := util.VolumeStoreNameToURL(storeName)
	if err != nil {
		return nil, err
	}

	v := &VolumeStore{
		dm:       dm,
		ds:       ds,
		SelfLink: u,
	}

	return v, nil
}

// Returns the path to the vol relative to the given store.  The dir structure
// for a vol in the datastore is `<configured datastore path>/volumes/<vol ID>/<vol ID>.vmkd`.
// Everything up to "volumes" is taken care of by the datastore wrapper.
func (v *VolumeStore) volDirPath(ID string) string {
	return path.Join(VolumesDir, ID)
}

// Returns the path to the metadata directory for a volume
func (v *VolumeStore) volMetadataDirPath(ID string) string {
	return path.Join(v.volDirPath(ID), metaDataDir)
}

// Returns the path to the vmdk itself (in datastore URL format)
func (v *VolumeStore) volDiskDsURL(ID string) *object.DatastorePath {
	return &object.DatastorePath{
		Datastore: v.ds.RootURL.Datastore,
		Path:      path.Join(v.ds.RootURL.Path, v.volDirPath(ID), ID+".vmdk"),
	}
}

func (v *VolumeStore) VolumeCreate(op trace.Operation, ID string, store *url.URL, capacityKB uint64, info map[string][]byte) (*storage.Volume, error) {

	// Create the volume directory in the store.
	if _, err := v.ds.Mkdir(op, false, v.volDirPath(ID)); err != nil {
		return nil, err
	}

	// Get the path to the disk in datastore uri format
	volDiskDsURL := v.volDiskDsURL(ID)

	config := disk.NewPersistentDisk(volDiskDsURL).WithCapacity(int64(capacityKB))
	// Create the disk
	vmdisk, err := v.dm.CreateAndAttach(op, config)
	if err != nil {
		return nil, err
	}
	defer v.dm.Detach(op, vmdisk.VirtualDiskConfig)
	vol, err := storage.NewVolume(store, ID, info, vmdisk, executor.CopyNew)
	if err != nil {
		return nil, err
	}

	// Make the filesystem and set its label
	if err = vmdisk.Mkfs(vol.Label); err != nil {
		return nil, err
	}

	// Persist the metadata
	metaDataDir := v.volMetadataDirPath(ID)
	if err = writeMetadata(op, v.ds, metaDataDir, info); err != nil {
		return nil, err
	}

	op.Infof("volumestore: %s (%s)", ID, vol.SelfLink)
	return vol, nil
}

func (v *VolumeStore) VolumeDestroy(op trace.Operation, vol *storage.Volume) error {
	volDir := v.volDirPath(vol.ID)

	op.Infof("VolumeStore: Deleting %s", volDir)
	if err := v.ds.Rm(op, volDir); err != nil {
		op.Errorf("VolumeStore: delete error: %s", err.Error())
		return err
	}

	return nil
}

func (v *VolumeStore) VolumeGet(op trace.Operation, ID string) (*storage.Volume, error) {
	// We can't get the volume directly without looking up what datastore it's on.
	return nil, fmt.Errorf("not supported: use VolumesList")
}

func (v *VolumeStore) VolumesList(op trace.Operation) ([]*storage.Volume, error) {
	volumes := []*storage.Volume{}

	res, err := v.ds.Ls(op, VolumesDir)
	if err != nil {
		return nil, fmt.Errorf("error listing vols: %s", err)
	}

	for _, f := range res.File {
		file, ok := f.(*types.FileInfo)
		if !ok {
			continue
		}

		ID := file.Path

		// Get the path to the disk in datastore uri format
		volDiskDsURL := v.volDiskDsURL(ID)

		config := disk.NewPersistentDisk(volDiskDsURL)
		dev, err := disk.NewVirtualDisk(config, v.dm.Disks)
		if err != nil {
			return nil, err
		}

		metaDataDir := v.volMetadataDirPath(ID)
		meta, err := getMetadata(op, v.ds, metaDataDir)
		if err != nil {
			return nil, err
		}

		vol, err := storage.NewVolume(v.SelfLink, ID, meta, dev, executor.CopyNew)
		if err != nil {
			return nil, err
		}

		volumes = append(volumes, vol)
	}

	return volumes, nil
}

func (v *VolumeStore) StatPath(op trace.Operation, deviceId string, target string) (stat *compute.FileStat, err error) {
	v.volDiskDsURL(deviceId)

	diskDsURI := v.volDiskDsURL(deviceId)
	config := disk.NewPersistentDisk(diskDsURI)
	dsk, err := v.dm.CreateAndAttach(op, config)
	if err != nil {
		op.Debugf("Failed to attach disk for the volume")
		return nil, err
	}

	defer func() {
		e1 := v.dm.Detach(op, config)
		if e1 != nil {
			if err == nil {
				err = e1
			}
		}
	}()

	// tmp dir to mount the disk
	dir, err := ioutil.TempDir("", "mntvol")
	if err != nil {
		err = errors.Errorf("Failed to create temp dir %s ", err.Error())
		return nil, err
	}

	defer func() {
		e1 := os.Remove(dir)
		if e1 != nil {
			op.Errorf("Failed to remove tempDir: %s", e1)
			err = errors.Errorf(compute.Test(dir))
			//if err == nil {
			//	err = e1
			//}
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

	return compute.InspectFileStat(path.Join(dir, target))
}


/*

// tmp dir to mount the disk
	dir, err := ioutil.TempDir("", "mnt-"+portlayer.Scratch.ID)
	if err != nil {
		op.Errorf("Failed to create tempDir: %s", err)
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

	if err = vmdisk.Mount(dir, nil); err != nil {
		op.Errorf("Failed to mount device %s to dir %s", vmdisk.DevicePath, dir)
		return err
	}

	defer func() {
		e2 := vmdisk.Unmount()
		if e2 != nil {
			op.Errorf("Failed to unmount device: %s", e2)
			if err == nil {
				err = e2
			}
		}
	}()


*/