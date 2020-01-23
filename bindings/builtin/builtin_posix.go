// +build !windows
// +build !local_rx_build

package builtin

// #cgo pkg-config: libreindexer
// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable
// #cgo LDFLAGS: -g
import "C"
