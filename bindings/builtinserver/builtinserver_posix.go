// +build !windows
// +build !local_rx_build

package builtinserver

// #cgo pkg-config: libreindexer_server
// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable
// #cgo LDFLAGS: -g
import "C"
