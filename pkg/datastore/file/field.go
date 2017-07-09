package file

import (
	"fmt"
	"strings"
)

// Field contains name and format of every data item
type Field struct {
	Topic  string
	Name   string
	Type   string
	Format string
}

// Fields declared as type which used in decoder
type Fields []Field

// Decode used as decoder for type Fields
func (f *Fields) Decode(value string) error {
	if strings.Trim(value, " ") == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(c rune) bool { return c == ',' || c == ' ' })
	for _, part := range parts {
		kv := strings.Split(part, ":")
		switch len(kv) {
		case 3:
			*f = append(*f, Field{"", kv[0], kv[1], kv[2]})
		case 4:
			*f = append(*f, Field{kv[0], kv[1], kv[2], kv[3]})
		default:
			return fmt.Errorf("invalid struct item: %q", part)
		}
	}

	return nil
}
