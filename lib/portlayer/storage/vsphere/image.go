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
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/docker/docker/pkg/archive"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/portlayer/exec"
	portlayer "github.com/vmware/vic/lib/portlayer/storage"
	"github.com/vmware/vic/lib/portlayer/util"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/vsphere/datastore"
	"github.com/vmware/vic/pkg/vsphere/disk"
	"github.com/vmware/vic/pkg/vsphere/session"
	"github.com/vmware/vic/lib/portlayer/storage/compute"
	"github.com/vmware/vic/pkg/vsphere/sys"
)

// All paths on the datastore for images are relative to <datastore>/VIC/
var StorageParentDir = "VIC"

// Set to false for unit tests
var (
	DetachAll = true

	FileForMinOS = map[string]os.FileMode{
		"/etc/hostname":    0644,
		"/etc/hosts":       0644,
		"/etc/resolv.conf": 0644,
	}
	// Here the permission of .tether should be drwxrwxrwt.
	// The sticky bit 't' is added when mounting the tmpfs in bootstrap
	DirForMinOS = map[string]os.FileMode{
		"/etc":         0755,
		"/lib/modules": 0755,
		"/proc":        0555,
		"/sys":         0555,
		"/.tether":     0777,
	}
)

const (
	StorageImageDir     = "images"
	defaultDiskLabel    = "containerfs"
	defaultDiskSizeInKB = 8 * 1024 * 1024
	metaDataDir         = "imageMetadata"
	manifest            = "manifest"
)

type ImageStore struct {
	dm *disk.Manager

	// govmomi session
	s *session.Session

	ds *datastore.Helper
}

func NewImageStore(op trace.Operation, s *session.Session, u *url.URL) (*ImageStore, error) {
	dm, err := disk.NewDiskManager(op, s)
	if err != nil {
		return nil, err
	}

	if DetachAll {
		if err = dm.DetachAll(op); err != nil {
			return nil, err
		}
	}

	datastores, err := s.Finder.DatastoreList(op, u.Host)
	if err != nil {
		return nil, fmt.Errorf("Host returned error when trying to locate provided datastore %s: %s", u.String(), err.Error())
	}

	if len(datastores) != 1 {
		return nil, fmt.Errorf("Found %d datastores with provided datastore path %s. Cannot create image store.", len(datastores), u)
	}

	ds, err := datastore.NewHelper(op, s, datastores[0], path.Join(u.Path, StorageParentDir))
	if err != nil {
		return nil, err
	}

	vis := &ImageStore{
		dm: dm,
		ds: ds,
		s:  s,
	}

	return vis, nil
}

// Returns the path to a given image store.  Currently this is the UUID of the VCH.
// `/VIC/imageStoreName (currently the vch uuid)/images`
func (v *ImageStore) imageStorePath(storeName string) string {
	return path.Join(storeName, StorageImageDir)
}

// Returns the path to the image relative to the given
// store.  The dir structure for an image in the datastore is
// `/VIC/imageStoreName (currently the vch uuid)/imageName/imageName.vmkd`
func (v *ImageStore) imageDirPath(storeName, imageName string) string {
	return path.Join(v.imageStorePath(storeName), imageName)
}

func (v *ImageStore) imageDiskPath(storeName, imageName string) string {
	return path.Join(v.imageDirPath(storeName, imageName), imageName+".vmdk")
}

// Returns the path to the vmdk itself in datastore url format
func (v *ImageStore) imageDiskDSPath(storeName, imageName string) *object.DatastorePath {
	return &object.DatastorePath{
		Datastore: v.ds.RootURL.Datastore,
		Path:      path.Join(v.ds.RootURL.Path, v.imageDiskPath(storeName, imageName)),
	}
}

// Returns the path to the metadata directory for an image
func (v *ImageStore) imageMetadataDirPath(storeName, imageName string) string {
	return path.Join(v.imageDirPath(storeName, imageName), metaDataDir)
}

// Returns the path to the manifest file.  This file is our "done" file.
func (v *ImageStore) manifestPath(storeName, imageName string) string {
	return path.Join(v.imageDirPath(storeName, imageName), manifest)
}

func (v *ImageStore) CreateImageStore(op trace.Operation, storeName string) (*url.URL, error) {
	// convert the store name to a port layer url.
	u, err := util.ImageStoreNameToURL(storeName)
	if err != nil {
		return nil, err
	}

	if _, err = v.ds.Mkdir(op, true, v.imageStorePath(storeName)); err != nil {
		return nil, err
	}

	return u, nil
}

// DeleteImageStore deletes the image store top level directory
func (v *ImageStore) DeleteImageStore(op trace.Operation, storeName string) error {
	op.Infof("Cleaning up image store %s", storeName)
	return v.ds.Rm(op, v.imageStorePath(storeName))
}

// GetImageStore checks to see if the image store exists on disk and returns an
// error or the store's URL.
func (v *ImageStore) GetImageStore(op trace.Operation, storeName string) (*url.URL, error) {
	u, err := util.ImageStoreNameToURL(storeName)
	if err != nil {
		return nil, err
	}

	p := v.imageStorePath(storeName)
	info, err := v.ds.Stat(op, p)
	if err != nil {
		return nil, err
	}

	_, ok := info.(*types.FolderFileInfo)
	if !ok {
		return nil, fmt.Errorf("Stat error:  path doesn't exist (%s)", p)
	}

	// This is startup.  Look for image directories without manifest files and
	// nuke them.
	if err := v.cleanup(op, u); err != nil {
		return nil, err
	}

	return u, nil
}

func (v *ImageStore) ListImageStores(op trace.Operation) ([]*url.URL, error) {
	res, err := v.ds.Ls(op, v.imageStorePath(""))
	if err != nil {
		return nil, err
	}

	stores := []*url.URL{}
	for _, f := range res.File {
		folder, ok := f.(*types.FolderFileInfo)
		if !ok {
			continue
		}
		u, err := util.ImageStoreNameToURL(folder.Path)
		if err != nil {
			return nil, err
		}
		stores = append(stores, u)

	}

	return stores, nil
}

// WriteImage creates a new image layer from the given parent.
// Eg parentImage + newLayer = new Image built from parent
//
// parent - The parent image to create the new image from.
// ID - textual ID for the image to be written
// meta - metadata associated with the image
// Tag - the tag of the image to be written
func (v *ImageStore) WriteImage(op trace.Operation, parent *portlayer.Image, ID string, meta map[string][]byte, sum string,
	r io.Reader) (*portlayer.Image, error) {

	storeName, err := util.ImageStoreName(parent.Store)
	if err != nil {
		return nil, err
	}

	imageURL, err := util.ImageURL(storeName, ID)
	if err != nil {
		return nil, err
	}

	var dsk *disk.VirtualDisk
	// If this is scratch, then it's the root of the image store.  All images
	// will be descended from this created and prepared fs.
	if ID == portlayer.Scratch.ID {
		// Create the scratch layer
		if err := v.scratch(op, storeName); err != nil {
			return nil, err
		}
	} else {

		if parent.ID == "" {
			return nil, fmt.Errorf("parent ID is empty")
		}

		dsk, err = v.writeImage(op, storeName, parent.ID, ID, meta, sum, r)
		if err != nil {
			return nil, err
		}
	}

	newImage := &portlayer.Image{
		ID:         ID,
		SelfLink:   imageURL,
		ParentLink: parent.SelfLink,
		Store:      parent.Store,
		Metadata:   meta,
		Disk:       dsk,
	}

	return newImage, nil
}

// cleanup safely on error
func (v *ImageStore) cleanupDisk(op trace.Operation, ID, storeName string, vmdisk *disk.VirtualDisk) {
	op.Errorf("Cleaning up failed image %s", ID)

	if vmdisk != nil {
		if vmdisk.Mounted() {
			op.Debugf("Unmounting abandoned disk")
			vmdisk.Unmount()
		}

		if vmdisk.Attached() {
			op.Debugf("Detaching abandoned disk")
			v.dm.Detach(op, vmdisk.VirtualDiskConfig)
		}
	}

	v.deleteImage(op, storeName, ID)
}

// Create the image directory, create a temp vmdk in this directory,
// attach/mount the disk, unpack the tar, check the checksum.  If the data
// doesn't match the expected checksum, abort by nuking the image directory.
// If everything matches, move the tmp vmdk to ID.vmdk.  The unwind path is a
// bit convoluted here;  we need to clean up on the way out in the error case
func (v *ImageStore) writeImage(op trace.Operation, storeName, parentID, ID string, meta map[string][]byte,
	sum string, r io.Reader) (*disk.VirtualDisk, error) {

	// Create a temp image directory in the store.
	imageDir := v.imageDirPath(storeName, ID)
	_, err := v.ds.Mkdir(op, true, imageDir)
	if err != nil {
		return nil, err
	}

	// Write the metadata to the datastore
	metaDataDir := v.imageMetadataDirPath(storeName, ID)
	err = writeMetadata(op, v.ds, metaDataDir, meta)
	if err != nil {
		return nil, err
	}

	// datastore path to the parent
	parentDiskDsURI := v.imageDiskDSPath(storeName, parentID)

	// datastore path to the disk we're creating
	diskDsURI := v.imageDiskDSPath(storeName, ID)
	op.Infof("Creating image %s (%s)", ID, diskDsURI)

	var vmdisk *disk.VirtualDisk
	// On error, unmount if mounted, detach if attached, and nuke the image directory
	defer func() {
		if err == nil {
			return
		}
		v.cleanupDisk(op, ID, storeName, vmdisk)
	}()

	config := disk.NewPersistentDisk(diskDsURI).WithParent(parentDiskDsURI)
	// Create the disk
	vmdisk, err = v.dm.CreateAndAttach(op, config)
	if err != nil {
		return nil, err
	}
	// tmp dir to mount the disk
	dir, err := ioutil.TempDir("", "mnt-"+ID)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	if err := vmdisk.Mount(dir, nil); err != nil {
		return nil, err
	}

	h := sha256.New()
	t := io.TeeReader(r, h)

	// Untar the archive
	var n int64
	if n, err = archive.ApplyLayer(dir, t); err != nil {
		return nil, err
	}

	op.Debugf("%s wrote %d bytes", ID, n)

	actualSum := fmt.Sprintf("sha256:%x", h.Sum(nil))
	if actualSum != sum {
		err = fmt.Errorf("Failed to validate image checksum. Expected %s, got %s", sum, actualSum)
		return nil, err
	}

	if err = vmdisk.Unmount(); err != nil {
		return nil, err
	}

	if err = v.dm.Detach(op, vmdisk.VirtualDiskConfig); err != nil {
		return nil, err
	}

	// Write our own bookkeeping manifest file to the image's directory.  We
	// treat the manifest file like a done file.  Its existence means this vmdk
	// is consistent.  Previously we were writing the vmdk to a tmp vmdk file
	// then moving it (using the MoveDatastoreFile or MoveVirtualDisk calls).
	// However(!!) this flattens the vmdk.  Also mkdir foo && ls -l foo fails
	// on VSAN (see
	// https://github.com/vmware/vic/pull/1764#issuecomment-237093424 for
	// detail).  We basically can't trust any of the datastore calls to help us
	// with atomic operations.  Touching an empty file seems to work well
	// enough.
	if err = v.writeManifest(op, storeName, ID, nil); err != nil {
		return nil, err
	}

	return vmdisk, nil
}

func (v *ImageStore) scratch(op trace.Operation, storeName string) error {
	var (
		vmdisk *disk.VirtualDisk
		size   int64
		err    error
	)

	// Create the image directory in the store.
	imageDir := v.imageDirPath(storeName, portlayer.Scratch.ID)
	if _, err := v.ds.Mkdir(op, false, imageDir); err != nil {
		return err
	}

	// Write the metadata to the datastore
	metaDataDir := v.imageMetadataDirPath(storeName, portlayer.Scratch.ID)
	if err := writeMetadata(op, v.ds, metaDataDir, nil); err != nil {
		return err
	}

	imageDiskDsURI := v.imageDiskDSPath(storeName, portlayer.Scratch.ID)
	op.Infof("Creating image %s (%s)", portlayer.Scratch.ID, imageDiskDsURI)

	size = defaultDiskSizeInKB
	if portlayer.Config.ScratchSize != 0 {
		size = portlayer.Config.ScratchSize
	}

	defer func() {
		if err == nil {
			return
		}
		v.cleanupDisk(op, portlayer.Scratch.ID, storeName, vmdisk)
	}()

	config := disk.NewPersistentDisk(imageDiskDsURI).WithCapacity(size)
	// Create the disk
	vmdisk, err = v.dm.CreateAndAttach(op, config)
	if err != nil {
		op.Errorf("CreateAndAttach(%s) error: %s", imageDiskDsURI, err)
		return err
	}

	op.Debugf("Scratch disk created with size %d", portlayer.Config.ScratchSize)

	// Make the filesystem and set its label to defaultDiskLabel
	if err = vmdisk.Mkfs(defaultDiskLabel); err != nil {
		return err
	}

	if err = createBaseStructure(op, vmdisk); err != nil {
		return err
	}

	if err = v.dm.Detach(op, vmdisk.VirtualDiskConfig); err != nil {
		return err
	}

	if err = v.writeManifest(op, storeName, portlayer.Scratch.ID, nil); err != nil {
		return err
	}

	return nil
}

func (v *ImageStore) GetImage(op trace.Operation, store *url.URL, ID string) (*portlayer.Image, error) {
	defer trace.End(trace.Begin(store.String() + "/" + ID))
	storeName, err := util.ImageStoreName(store)
	if err != nil {
		return nil, err
	}

	imageURL, err := util.ImageURL(storeName, ID)
	if err != nil {
		return nil, err
	}

	if err = v.verifyImage(op, storeName, ID); err != nil {
		return nil, err
	}

	// get the metadata
	metaDataDir := v.imageMetadataDirPath(storeName, ID)
	meta, err := getMetadata(op, v.ds, metaDataDir)
	if err != nil {
		return nil, err
	}

	diskDsURI := v.imageDiskDSPath(storeName, ID)

	var s = *store

	config := disk.NewPersistentDisk(diskDsURI)
	dsk, err := v.dm.Get(op, config)
	if err != nil {
		return nil, err
	}

	var parentURL *url.URL
	if dsk.ParentDatastoreURI != nil {
		vmdk := path.Base(dsk.ParentDatastoreURI.Path)
		parentURL, err = util.ImageURL(storeName, strings.TrimSuffix(vmdk, path.Ext(vmdk)))
		if err != nil {
			return nil, err
		}
	}

	newImage := &portlayer.Image{
		ID:         ID,
		SelfLink:   imageURL,
		Store:      &s,
		ParentLink: parentURL,
		Metadata:   meta,
		Disk:       dsk,
	}

	op.Debugf("GetImage(%s) has parent %s", newImage.SelfLink, newImage.Parent())
	return newImage, nil
}

func (v *ImageStore) ListImages(op trace.Operation, store *url.URL, IDs []string) ([]*portlayer.Image, error) {

	storeName, err := util.ImageStoreName(store)
	if err != nil {
		return nil, err
	}

	res, err := v.ds.Ls(op, v.imageStorePath(storeName))
	if err != nil {
		return nil, err
	}

	images := []*portlayer.Image{}
	for _, f := range res.File {
		file, ok := f.(*types.FileInfo)
		if !ok {
			continue
		}

		ID := file.Path

		// filter out scratch
		if ID == portlayer.Scratch.ID {
			continue
		}

		// GetImage verifies the image is good by calling verifyImage.
		img, err := v.GetImage(op, store, ID)
		if err != nil {
			return nil, err
		}

		images = append(images, img)
	}

	return images, nil
}

// DeleteImage deletes an image from the image store.  If the image is in
// use either by way of inheritance or because it's attached to a
// container, this will return an error.
func (v *ImageStore) DeleteImage(op trace.Operation, image *portlayer.Image) (*portlayer.Image, error) {
	//  check if the image is in use.
	if err := imagesInUse(op, image.ID); err != nil {
		op.Errorf("ImageStore: delete image error: %s", err.Error())
		return nil, err
	}

	storeName, err := util.ImageStoreName(image.Store)
	if err != nil {
		return nil, err
	}

	return image, v.deleteImage(op, storeName, image.ID)
}

func (v *ImageStore) deleteImage(op trace.Operation, storeName, ID string) error {
	// Delete in order of manifest (the done file), the vmdk (because VC honors
	// the deletable flag in the vmdk file), then the directory to get
	// everything else.
	paths := []string{
		v.manifestPath(storeName, ID),
		v.imageDiskPath(storeName, ID),
		v.imageDirPath(storeName, ID),
	}

	for _, pth := range paths {
		err := v.ds.Rm(op, pth)

		// not exist is ok
		if err == nil || types.IsFileNotFound(err) {
			continue
		}

		// something isn't right.  bale.
		op.Errorf("ImageStore: delete image error: %s", err.Error())
		return err
	}

	return nil
}

// Find any image directories without the manifest file and remove them.
func (v *ImageStore) cleanup(op trace.Operation, store *url.URL) error {
	op.Infof("Checking for inconsistent images on %s", store.String())

	storeName, err := util.ImageStoreName(store)
	if err != nil {
		return err
	}

	res, err := v.ds.Ls(op, v.imageStorePath(storeName))
	if err != nil {
		return err
	}

	// We could call v.ListImages here but that results in calling GetImage,
	// which pulls and unmarshalls the metadata.  We don't need that.
	for _, f := range res.File {
		file, ok := f.(*types.FileInfo)
		if !ok {
			continue
		}

		ID := file.Path

		if ID == portlayer.Scratch.ID {
			continue
		}

		if err := v.verifyImage(op, storeName, ID); err != nil {

			if err = v.deleteImage(op, storeName, ID); err != nil {
				// deleteImage logs the error in the event there is one.
				return err
			}
		}
	}

	return nil
}

// Manifest file for the image.
func (v *ImageStore) writeManifest(op trace.Operation, storeName, ID string, r io.Reader) error {

	if err := v.ds.Upload(op, r, v.manifestPath(storeName, ID)); err != nil {
		return err
	}

	return nil
}

// check for the manifest file AND the vmdk
func (v *ImageStore) verifyImage(op trace.Operation, storeName, ID string) error {

	// Check for the manifiest file and the vmdk
	for _, p := range []string{v.manifestPath(storeName, ID), v.imageDiskPath(storeName, ID)} {
		if _, err := v.ds.Stat(op, p); err != nil {
			return err
		}
	}

	return nil
}

// XXX TODO This should be tied to an interface so we don't have to import exec
// here (or wherever the cache lives).
func imagesInUse(op trace.Operation, ID string) error {
	// XXX Why doesnt this ever return an error?  Strange.
	// Gather all containers
	conts := exec.Containers.Containers(nil)
	if len(conts) == 0 {
		return nil
	}

	for _, cont := range conts {
		layerID := cont.ExecConfig.LayerID

		if layerID == ID {
			return &portlayer.ErrImageInUse{
				Msg: fmt.Sprintf("image %s in use by %s", ID, cont.ExecConfig.ID),
			}
		}
	}

	return nil
}

// populate the scratch with minimum OS structure defined in FileForMinOS and DirForMinOS
func createBaseStructure(op trace.Operation, vmdisk *disk.VirtualDisk) (err error) {
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

	for dname, dmode := range DirForMinOS {
		dirPath := path.Join(dir, dname)
		if err = os.MkdirAll(dirPath, dmode); err != nil {
			op.Errorf("Failed to create directory %s: %s", dirPath, err)
			return err
		}
	}

	// The directory has to exist before creating the new file
	for fname, fmode := range FileForMinOS {
		filePath := path.Join(dir, fname)
		f, err := os.OpenFile(filePath, os.O_CREATE, fmode)
		if err != nil {
			op.Errorf("Failed to open file %s: %s", filePath, err)
			return err
		}

		err = f.Close()
		if err != nil {
			op.Errorf("Failed to close file %s: %s", filePath, err)
			return err
		}
	}

	return nil
}

//
func (v *ImageStore) StatPath(op trace.Operation, deviceId string, target string) (*compute.FileStat, error) {
	/*

	get image store, get image, if it exists, do create and attach on its disk manager
	the returned virtual disk if error is not nil, has functions to see mount path,
	so I can join the path string and use os.filestat on it.

	*/

	// verify that disk
	host, err := sys.UUID()
	if err != nil {
		op.Debugf("Failed to determine host UUID")
		return nil, err
	}

	if err = v.verifyImage(op, host, deviceId); err != nil {
		op.Debugf("Device is not an image")
		return nil, err
	}

	// TODO: not sure if these checks are needed
	//store, err := v.GetImageStore(op, host)
	//if err != nil {
	//	op.Debugf("Failed to get image store")
	//	return nil, err
	//}
	//
	//_, err = v.GetImage(op, store, deviceId)
	//if err != nil {
	//	op.Debugf("Device is not an image")
	//	return nil, err
	//}
\
	diskDsURI := v.imageDiskDSPath(host, deviceId)
	config := disk.NewPersistentDisk(diskDsURI)
	dsk, err := v.dm.CreateAndAttach(op, config)
	if err != nil {
		op.Debugf("Failed to attach disk for the image")
		return nil, err
	}

	dsk.Mount("/tmpfs/", nil)

	// need to find the config

	//op trace.Operation, config *VirtualDiskConfig

	return nil, nil
}