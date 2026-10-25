// Package pool reuses the 32KiB buffers used to copy upstream bodies.
// Putting the pointer (not the slice) in the pool avoids an allocation on
// every Put; see staticcheck SA6002.
package pool

import "sync"

const Size = 32 * 1024

var bufs = sync.Pool{
	New: func() any {
		b := make([]byte, Size)
		return &b
	},
}

func Get() *[]byte {
	return bufs.Get().(*[]byte)
}

func Put(b *[]byte) {
	if b == nil || cap(*b) < Size {
		return
	}
	*b = (*b)[:Size]
	bufs.Put(b)
}
