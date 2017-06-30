package backends

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/gob"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/tchap/go-patricia/patricia"

	"github.com/vmware/vic/lib/apiservers/engine/backends/cache"
	viccontainer "github.com/vmware/vic/lib/apiservers/engine/backends/container"
	"github.com/vmware/vic/pkg/trace"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/api/types"
	"github.com/vmware/vic/lib/apiservers/engine/backends/cache"
	viccontainer "github.com/vmware/vic/lib/apiservers/engine/backends/container"
	"github.com/vmware/vic/pkg/trace"
)

// ContainerArchivePath creates an archive of the filesystem resource at the
// specified path in the container identified by the given name. Returns a
// tar archive of the resource and whether it was a directory or a single file.
func (c *Container) ContainerArchivePath(name string, path string) (content io.ReadCloser, stat *types.ContainerPathStat, err error) {
	defer trace.End(trace.Begin(name))

	vc := cache.ContainerCache().GetContainer(name)
	if vc == nil {
		return nil, nil, NotFoundError(name)
	}

	stat, err := c.ContainerStatPath(name, path)
	if err != nil {
		return nil, nil, err
	}

	mounts := mountsFromContainer(vc)
	mounts = append(mounts, types.MountPoint{Destination: "/"})
	readerMap := NewArchiveStreamReaderMap(vc, mounts, path)

	var readers []io.Reader
	for _, reader := range readerMap.Readers() {
		readers = append(readers, reader)
	}

	finalTarReader := io.MultiReader(readers...)

	return ioutil.NopCloser(finalTarReader), stat, nil
)

// ContainerCopy performs a deprecated operation of archiving the resource at
// the specified path in the container identified by the given name.
func (c *Container) ContainerCopy(name string, res string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("%s does not yet implement ContainerCopy", ProductName())
}

// ContainerExport writes the contents of the container to the given
// writer. An error is returned if the container cannot be found.
func (c *Container) ContainerExport(name string, out io.Writer) error {
	return fmt.Errorf("%s does not yet implement ContainerExport", ProductName())
}

// ContainerExtractToDir extracts the given archive to the specified location
// in the filesystem of the container identified by the given name. The given
// path must be of a directory in the container. If it is not, the error will
// be ErrExtractPointNotDirectory. If noOverwriteDirNonDir is true then it will
// be an error if unpacking the given content would cause an existing directory
// to be replaced with a non-directory and vice versa.
func (c *Container) ContainerExtractToDir(name, path string, noOverwriteDirNonDir bool, content io.Reader) error {
	defer trace.End(trace.Begin(name))

	vc := cache.ContainerCache().GetContainer(name)
	if vc == nil {
		return NotFoundError(name)
	}

	rawReader, err := archive.DecompressStream(content)
	if err != nil {
		log.Errorf("Input tar stream to docker cp not recognized: %s", err.Error())
		return StreamFormatNotRecognized()
	}
	tarReader := tar.NewReader(rawReader)

	mounts := mountsFromContainer(vc)
	mounts = append(mounts, types.MountPoint{Destination: "/"})
	writerMap := NewArchiveStreamWriterMap(vc, mounts, path)

	for {
		header, err := tarReader.Next()
		if err != nil || err == io.EOF {
			break
		}

		// Figure out the mount prefix for current entry in the tar stream
		log.Debugf("tar header: %#v", *header)

		// Lookup the writer for that mount prefix
		writer, err := writerMap.WriterForAsset(c.containerProxy, path, *header)
		if err != nil {
			break
		}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(header)
		if err != nil {
			log.Errorf("Unable to encode header")
			continue
		}

		headerReader := bytes.NewReader(buf.Bytes())
		_, err = io.Copy(writer, headerReader)
		if err != nil {
			log.Errorf("Error while copying tar header: %s", err.Error())
		}

		_, err = io.Copy(writer, tarReader)
		if err != nil {
			log.Errorf("Error while copying tar data for %s: %s", header.Name, err.Error())
		}
	}

	writerMap.Close()

	return nil
}

// ContainerStatPath stats the filesystem resource at the specified path in the
// container identified by the given name.
func (c *Container) ContainerStatPath(name string, path string) (*types.ContainerPathStat, error) {
	defer trace.End(trace.Begin(name))

	vc := cache.ContainerCache().GetContainer(name)
	if vc == nil {
		return nil, NotFoundError(name)
	}

	stat, err := c.containerProxy.StatPath(context.Background(), name, path)
	if err != nil {
		return nil, err
	}
	log.Debugf("%#v", stat)
	return stat, nil
}

//----------------------------------
// Docker cp utility
//----------------------------------

type ArchiveWriter struct {
	mountPoint   types.MountPoint
	strippedDest string
	writer       io.WriteCloser
}

// ArchiveStreamWriterMap maps mount prefix to io.WriteCloser
type ArchiveStreamWriterMap struct {
	prefixTrie *patricia.Trie
}

// ArchiveStreamReaderMap maps mount prefix to io.ReadCloser
type ArchiveStreamReaderMap struct {
	prefixTrie *patricia.Trie
}

// NewArchiveStreamWriterMap creates a new ArchiveStreamWriterMap
func NewArchiveStreamWriterMap(vc *viccontainer.VicContainer, mounts []types.MountPoint, destPath string) *ArchiveStreamWriterMap {
	writerMap := &ArchiveStreamWriterMap{}
	writerMap.prefixTrie = patricia.NewTrie()

	for _, m := range mounts {
		// Destination to ImportStream must remove the mountPrefix.  For example,
		// if user specified cid:/mnt/A/data during docker cp, /mnt/A is the mount point, but the
		// volume doesn't know about /mnt/A.  Only the Persona and the container knows the volume
		// is mounted at /mnt/A.  We must strip of /mnt/A from the destination and simply pass
		// 'data' to the storage portlayer.
		strippedDestination := strings.TrimPrefix(destPath, m.Destination)
		log.Infof("** stripping prefix %s from dest %s: %s", m.Destination, destPath, strippedDestination)

		aw := ArchiveWriter{
			mountPoint:   m,
			writer:       nil,
			strippedDest: strippedDestination,
		}

		log.Infof("** building prefix trie: objectid = %s", m.Destination)
		writerMap.prefixTrie.Insert(patricia.Prefix(m.Destination), &aw)
	}

	return writerMap
}

// WriterForAsset takes a destination path and subpath of the archive data and returns the
// appropriate writer for the two.  It's intention is to solve the case where there exist
// a mount point and another mount point within the first mount point.  For instance, the
// prefix map can have,
//
//		/mnt/a
//		/mnt/a/b
//
// If destination path is /mnt/a but subpath is b/file.txt, then the correct writer would
// be the one corresponding to prefix 2.
func (wm *ArchiveStreamWriterMap) WriterForAsset(proxy VicContainerProxy, dest string, assetHeader tar.Header) (io.WriteCloser, error) {
	defer trace.End(trace.Begin(assetHeader.Name))

	var aw *ArchiveWriter
	var err error
	var streamWriter io.WriteCloser

	// go function used later for searching
	findPrefix := func(prefix patricia.Prefix, item patricia.Item) error {
		if _, ok := item.(*ArchiveWriter); !ok {
			return fmt.Errorf("item not ArchiveWriter")
		}

		aw, _ = item.(*ArchiveWriter)
		log.Infof("%q\n", prefix)
		return nil
	}

	// Find the prefix for the
	prefix := patricia.Prefix(dest)
	err = wm.prefixTrie.VisitPrefixes(prefix, findPrefix)
	if err != nil {
		log.Errorf(err.Error())
		return nil, fmt.Errorf("Failed to find a node for prefix %s: %s", dest, err.Error())
	}

	log.Debugf("** Setting writer to %#v", aw)

	// Perform the lazy initialization here.
	if aw.writer == nil {
		// lazy initialize.
		filterSpec := make(map[string]string) //filterspec is empty for docker
		log.Debugf("Lazily initializing import stream for %s", aw.mountPoint.Name)
		streamWriter, err = proxy.ImportWriter(context.Background(), aw.mountPoint.Name, aw.strippedDest, filterSpec)
		if err == nil {
			log.Infof("Lazy initialization created writer %#v", streamWriter)
			aw.writer = streamWriter
		} else {
			err = fmt.Errorf("Unable to initalize import stream writer for prefix %s", aw.strippedDest)
			log.Errorf(err.Error())
			return nil, err
		}
	} else {
		streamWriter = aw.writer
	}

	return streamWriter, nil
}

// Close visits all the archive writer in the trie and closes the actual io.Writer
func (wm *ArchiveStreamWriterMap) Close() {
	closeStream := func(prefix patricia.Prefix, item patricia.Item) error {
		if aw, ok := item.(*ArchiveWriter); ok && aw.writer != nil {
			log.Infof("** closing archive writer - %#v", aw)
			aw.writer.Close()
		}
		return nil
	}

	wm.prefixTrie.Visit(closeStream)
}

// NewArchiveStreamReaderMap creates a new ArchiveStreamReaderMap
func NewArchiveStreamReaderMap(vc *viccontainer.VicContainer, mounts []types.MountPoint, source string) *ArchiveStreamReaderMap {
	return &ArchiveStreamReaderMap{}
}

func (wm *ArchiveStreamReaderMap) ReaderForAsset(proxy VicContainerProxy, source string) (io.ReadCloser, error) {
	return nil, nil
}

// Readers returns an existing or creates a new Reader for mount prefix
func (rm *ArchiveStreamReaderMap) Readers() []io.Reader {
	return nil
}
