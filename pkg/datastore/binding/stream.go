package binding

import (
	"bufio"
	"os"
)

// Stream provides files communication data
type Stream struct {
	Handle *os.File
	Writer *bufio.Writer
	Reader *bufio.Reader
}
