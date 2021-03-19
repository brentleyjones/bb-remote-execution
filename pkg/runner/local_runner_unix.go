// +build darwin freebsd linux

package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/resourceusage"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// logFileResolver is an implementation of path.ComponentWalker that is
// used by localRunner.Run() to traverse to the directory of stdout and
// stderr log files, so that they may be opened.
//
// TODO: This code seems fairly generic. Should move it to the
// filesystem package?
type logFileResolver struct {
	stack []filesystem.DirectoryCloser
	name  *path.Component
}

func (r *logFileResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	d := r.stack[len(r.stack)-1]
	child, err := d.EnterDirectory(name)
	if err != nil {
		return nil, err
	}
	r.stack = append(r.stack, child)
	return path.GotDirectory{
		Child:        r,
		IsReversible: true,
	}, nil
}

func (r *logFileResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	r.name = &name
	return nil, nil
}

func (r *logFileResolver) OnUp() (path.ComponentWalker, error) {
	if len(r.stack) == 1 {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the build directory")
	}
	if err := r.stack[len(r.stack)-1].Close(); err != nil {
		return nil, err
	}
	r.stack = r.stack[:len(r.stack)-1]
	return r, nil
}

func (r *logFileResolver) closeAll() {
	for _, d := range r.stack {
		d.Close()
	}
}

type localRunner struct {
	buildDirectory               filesystem.Directory
	buildDirectoryPath           *path.Builder
	sysProcAttr                  *syscall.SysProcAttr
	setTmpdirEnvironmentVariable bool
	chrootIntoInputRoot          bool
	developerDirs                map[string]string
}

// NewLocalRunner returns a Runner capable of running commands on the
// local system directly.
func NewLocalRunner(buildDirectory filesystem.Directory, buildDirectoryPath *path.Builder, sysProcAttr *syscall.SysProcAttr, setTmpdirEnvironmentVariable, chrootIntoInputRoot bool) Runner {
	return &localRunner{
		buildDirectory:               buildDirectory,
		buildDirectoryPath:           buildDirectoryPath,
		sysProcAttr:                  sysProcAttr,
		setTmpdirEnvironmentVariable: setTmpdirEnvironmentVariable,
		chrootIntoInputRoot:          chrootIntoInputRoot,
		developerDirs:                map[string]string{}, // TODO: Use shared cache between runners
	}
}

func (r *localRunner) openLog(logPath string) (filesystem.FileAppender, error) {
	logFileResolver := logFileResolver{
		stack: []filesystem.DirectoryCloser{filesystem.NopDirectoryCloser(r.buildDirectory)},
	}
	defer logFileResolver.closeAll()
	if err := path.Resolve(logPath, path.NewRelativeScopeWalker(&logFileResolver)); err != nil {
		return nil, err
	}
	if logFileResolver.name == nil {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a directory")
	}
	d := logFileResolver.stack[len(logFileResolver.stack)-1]
	return d.OpenAppend(*logFileResolver.name, filesystem.CreateExcl(0o666))
}

func (r *localRunner) resolveDeveloperDir(ctx context.Context, xcodeVersionOverride string) (string, error) {
	if resolvedDeveloperDir, ok := r.developerDirs[xcodeVersionOverride]; ok {
		return resolvedDeveloperDir, nil
	}

	var resolvedDeveloperDir string
	if xcodeVersionOverride == "" {
		out, err := exec.CommandContext(ctx, "/usr/bin/xcode-select", "-p").Output()
		if err != nil {
			return "", err
		}
		resolvedDeveloperDir = strings.TrimSpace(string(out))
	} else {
		ss := strings.Split(xcodeVersionOverride, ".")
		if len(ss) == 1 {
			return "", fmt.Errorf("XCODE_VERSION_OVERRIDE (%q) invalid format, expected SemVer.BuildVersion", xcodeVersionOverride)
		}
		xcodeBuildVersion := ss[len(ss)-1]

		out, err := exec.CommandContext(ctx, "/usr/bin/mdfind", "-onlyin", "/Applications/", "kMDItemCFBundleIdentifier == 'com.apple.dt.Xcode'").Output()
		if err != nil {
			return "", err
		}
		stdout := strings.TrimSpace(string(out))
		if len(stdout) == 0 {
			return "", errors.New("No Xcodes found")
		}

		for _, xcode := range strings.Split(stdout, "\n") {
			versionPlist := xcode + "/Contents/version.plist"
			out, err := exec.CommandContext(ctx, "/usr/libexec/PlistBuddy", "-c", "Print :ProductBuildVersion", versionPlist).Output()
			if err != nil {
				// We're just ignoring malformed Xcodes
				log.Println("Couldn't determine Build Version for", xcode)
				continue
			}
			if xcodeBuildVersion == strings.TrimSpace(string(out)) {
				resolvedDeveloperDir = xcode + "/Contents/Developer"
				break
			}
		}

		if resolvedDeveloperDir == "" {
			return "", fmt.Errorf("Cound not find Xcode with Build Version %q", xcodeBuildVersion)
		}
	}

	r.developerDirs[xcodeVersionOverride] = resolvedDeveloperDir
	return resolvedDeveloperDir, nil
}

func (r *localRunner) resolveXcodeEnvVars(ctx context.Context, request *runner.RunRequest) ([]string, error) {
	developerDir, err := r.resolveDeveloperDir(ctx, request.EnvironmentVariables["XCODE_VERSION_OVERRIDE"])
	if err != nil {
		return []string{}, err
	}

	var sdkPlatform string
	if sdkPlatformOverride := request.EnvironmentVariables["APPLE_SDK_PLATFORM"]; sdkPlatformOverride != "" {
		sdkPlatform = sdkPlatformOverride
	} else {
		sdkPlatform = "MacOSX"
	}

	sdkVersion := request.EnvironmentVariables["APPLE_SDK_VERSION_OVERRIDE"]

	sdkRoot := developerDir + "/Platforms/" + sdkPlatform + ".platform/Developer/SDKs/" + sdkPlatform + sdkVersion + ".sdk"

	return []string{"DEVELOPER_DIR=" + developerDir, "SDKROOT=" + sdkRoot}, nil
}

func convertTimeval(t syscall.Timeval) *durationpb.Duration {
	return &durationpb.Duration{
		Seconds: int64(t.Sec),
		Nanos:   int32(t.Usec) * 1000,
	}
}

func (r *localRunner) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	if len(request.Arguments) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient number of command arguments")
	}

	inputRootDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(request.InputRootDirectory, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve input root directory")
	}

	var cmd *exec.Cmd
	var workingDirectoryBase *path.Builder
	if r.chrootIntoInputRoot {
		// The addition of /usr/bin/env is necessary as the PATH resolution
		// will take place prior to the chroot, so the executable may not be
		// found by exec.LookPath() inside exec.CommandContext() and may
		// cause cmd.Start() to fail when it shouldn't.
		// https://github.com/golang/go/issues/39341
		envPrependedArguments := []string{"/usr/bin/env", "--"}
		envPrependedArguments = append(envPrependedArguments, request.Arguments...)
		cmd = exec.CommandContext(ctx, envPrependedArguments[0], envPrependedArguments[1:]...)
		sysProcAttr := *r.sysProcAttr
		sysProcAttr.Chroot = inputRootDirectory.String()
		cmd.SysProcAttr = &sysProcAttr
		workingDirectoryBase = &path.RootBuilder
	} else {
		cmd = exec.CommandContext(ctx, request.Arguments[0], request.Arguments[1:]...)
		cmd.SysProcAttr = r.sysProcAttr
		workingDirectoryBase = inputRootDirectory
	}

	// Set the environment variables.
	cmd.Env = make([]string, 0, len(request.EnvironmentVariables)+3)
	if r.setTmpdirEnvironmentVariable && request.TemporaryDirectory != "" {
		temporaryDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
		if err := path.Resolve(request.TemporaryDirectory, scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve temporary directory")
		}
		cmd.Env = append(cmd.Env, "TMPDIR="+temporaryDirectory.String())
	}
	for name, value := range request.EnvironmentVariables {
		cmd.Env = append(cmd.Env, name+"="+value)
	}

	xcodeEnvVars, err := r.resolveXcodeEnvVars(ctx, request)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve Xcode environment variables")
	}
	cmd.Env = append(cmd.Env, xcodeEnvVars...)

	// Set the working directory.
	workingDirectory, scopeWalker := workingDirectoryBase.Join(path.VoidScopeWalker)
	if err := path.Resolve(request.WorkingDirectory, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve working directory")
	}
	cmd.Dir = workingDirectory.String()

	// Open output files for logging.
	stdout, err := r.openLog(request.StdoutPath)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to open stdout")
	}
	cmd.Stdout = stdout

	stderr, err := r.openLog(request.StderrPath)
	if err != nil {
		stdout.Close()
		return nil, util.StatusWrap(err, "Failed to open stderr")
	}
	cmd.Stderr = stderr

	// Start the subprocess. We can already close the output files
	// while the process is running.
	err = cmd.Start()
	stdout.Close()
	stderr.Close()
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to start process")
	}

	// Wait for execution to complete. Permit non-zero exit codes.
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return nil, err
		}
	}

	// Attach rusage information to the response.
	rusage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	posixResourceUsage, err := anypb.New(&resourceusage.POSIXResourceUsage{
		UserTime:                   convertTimeval(rusage.Utime),
		SystemTime:                 convertTimeval(rusage.Stime),
		MaximumResidentSetSize:     int64(rusage.Maxrss) * maximumResidentSetSizeUnit,
		PageReclaims:               int64(rusage.Minflt),
		PageFaults:                 int64(rusage.Majflt),
		Swaps:                      int64(rusage.Nswap),
		BlockInputOperations:       int64(rusage.Inblock),
		BlockOutputOperations:      int64(rusage.Oublock),
		MessagesSent:               int64(rusage.Msgsnd),
		MessagesReceived:           int64(rusage.Msgrcv),
		SignalsReceived:            int64(rusage.Nsignals),
		VoluntaryContextSwitches:   int64(rusage.Nvcsw),
		InvoluntaryContextSwitches: int64(rusage.Nivcsw),
	})
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to marshal POSIX resource usage")
	}

	// Report signal exit codes correctly
	var exitCode int
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
		if status.Signaled() {
			exitCode = 128 + int(status.Signal())
		} else {
			exitCode = status.ExitStatus()
		}
	} else {
		exitCode = cmd.ProcessState.ExitCode()
	}

	return &runner.RunResponse{
		ExitCode:      int32(exitCode),
		ResourceUsage: []*anypb.Any{posixResourceUsage},
	}, nil
}
