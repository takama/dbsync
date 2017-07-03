package file

import (
	"fmt"
	"strings"
)

// Field contains name and format of every data item
type Field struct {
	name   string
	format string
}

// Fields declared as type which used in decoder
type Fields []Field

// Decode used as decoder for type Fields
func (f *Fields) Decode(value string) error {
	if strings.Trim(value, " ") == "" {
		return nil
	}
	pairs := strings.Split(value, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, ":")
		if len(kv) != 2 {
			return fmt.Errorf("invalid struct item: %q", pair)
		}
		*f = append(*f, Field{kv[0], kv[1]})
	}

	return nil
}
