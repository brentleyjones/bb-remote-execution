package builder

import (
	"log"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type stableSharedBuildDirectoryCreator struct {
	base     BuildDirectoryCreator
	workerID map[string]string
}

// TODO: Refactor common components out of SharedBuildDirectoryCreator
func NewStableSharedBuildDirectoryCreator(base BuildDirectoryCreator, workerID map[string]string) BuildDirectoryCreator {
	return &stableSharedBuildDirectoryCreator{
		base:     base,
		workerID: workerID,
	}
}

func (dc *stableSharedBuildDirectoryCreator) GetBuildDirectory(actionDigest digest.Digest, mayRunInParallel bool) (BuildDirectory, *path.Trace, error) {
	parentDirectory, parentDirectoryPath, err := dc.base.GetBuildDirectory(actionDigest, mayRunInParallel)
	if err != nil {
		return nil, nil, err
	}

	// Create the subdirectory.
	childDirectoryName := path.MustNewComponent(dc.workerID["thread"])
	childDirectoryPath := parentDirectoryPath.Append(childDirectoryName)
	if err := parentDirectory.Mkdir(childDirectoryName, 0o777); err != nil {
		parentDirectory.Close()
		return nil, nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create build directory %#v", childDirectoryPath.String())
	}
	childDirectory, err := parentDirectory.EnterBuildDirectory(childDirectoryName)
	if err != nil {
		if err := parentDirectory.Remove(childDirectoryName); err != nil {
			log.Printf("Failed to remove action digest build directory %#v upon failure to enter: %s", childDirectoryPath.String(), err)
		}
		parentDirectory.Close()
		return nil, nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter build directory %#v", childDirectoryPath.String())
	}

	return &sharedBuildDirectory{
		BuildDirectory:     childDirectory,
		parentDirectory:    parentDirectory,
		childDirectoryName: childDirectoryName,
		childDirectoryPath: childDirectoryPath.String(),
	}, childDirectoryPath, nil
}
