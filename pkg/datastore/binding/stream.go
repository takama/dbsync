package binding

import (
	"bufio"
	"compress/gzip"
	"os"
)

// Stream provides files communication data
type Stream struct {
	Handle   *os.File
	Writer   *bufio.Writer
	GZWriter *gzip.Writer
	Reader   *bufio.Reader
}
