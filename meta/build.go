package meta

// Version is the build artifact version injected with -ldflags. It is distinct
// from AppInfo.Version, which identifies the API contract (for example, v1).
// Keep this symbol and synchronize its full path across all Dockerfiles: the Go
// linker silently ignores an -X target that does not exist.
var Version = "dev"
