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

// Flow contains list of streams
type Flow map[string]Stream

// Close flow of the stream
func Close(flow Flow) (err error) {
	for ndx, stream := range flow {
		if stream.GZWriter != nil {
			err = stream.GZWriter.Close()
			if err != nil {
				return err
			}
		}
		if stream.Writer != nil {
			err = stream.Writer.Flush()
			if err != nil {
				return err
			}
		}
		if stream.Handle != nil {
			err = stream.Handle.Close()
			if err != nil {
				return err
			}
		}
		delete(flow, ndx)
	}

	return nil
}
