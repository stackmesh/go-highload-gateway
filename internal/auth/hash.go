// Package auth hashes API keys for the admission path.
//
// Keys are random tokens, not passwords, so SHA-256 is the right function:
// it is fast and the output is used as a map key. The hasher and the 32-byte
// digest buffer are pooled. The input string is passed to Write without
// copying it first. hash.Hash from crypto/sha256 copies the bytes into its
// own block and does not retain the slice, which is what makes the unsafe
// view sound.
package auth

import (
	"crypto/sha256"
	"hash"
	"sync"
	"unsafe"
)

var (
	hashers = sync.Pool{New: func() any { return sha256.New() }}
	sums    = sync.Pool{New: func() any {
		b := make([]byte, 32)
		return &b
	}}
)

// Sum returns the SHA-256 of token.
// The output array is filled from a pooled buffer so the digest does not
// escape into a fresh heap allocation on each call.
func Sum(token string) [32]byte {
	if token == "" {
		return sha256.Sum256(nil)
	}
	h := hashers.Get().(hash.Hash)
	h.Reset()
	_, _ = h.Write(unsafe.Slice(unsafe.StringData(token), len(token)))
	bp := sums.Get().(*[]byte)
	digest := h.Sum((*bp)[:0])
	var out [32]byte
	copy(out[:], digest)
	*bp = (*bp)[:32]
	sums.Put(bp)
	hashers.Put(h)
	return out
}
