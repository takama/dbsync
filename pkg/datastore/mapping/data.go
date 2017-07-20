package mapping

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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

// RenderMapping creates mapping
func (f *Fields) RenderMapping() (mapping string) {

	return
}

// RenderTxt generates plain text data
func RenderTxt(
	field Field, delimiter, finalizer string, useNames bool, quotas bool,
	columns []string, values []interface{},
) (data string) {
	const (
		timeTemplate = "2006-01-02 15:04:05"
		dateTemplate = "2006-01-02"
	)
	dlm := delimiter
	for ndx, name := range columns {
		if field.Name != "" && field.Name != name {
			continue
		}
		if value, ok := values[ndx].([]byte); ok {
			strValue := fmt.Sprintf(field.Format, string(value))
			if quotas {
				strValue = "\"" + strings.NewReplacer(
					"\"", "\\\"", "\\", "\\\\", "\n", "", "\r", "",
				).Replace(fmt.Sprintf(field.Format, string(value))) + "\""
			}
			if useNames {
				if quotas {
					dlm = "\"" + name + "\"" + delimiter
				} else {
					dlm = name + delimiter
				}
			}
			switch field.Type {
			case "string":
				data = data + dlm + strValue
			case "date":
				time, err := time.Parse(timeTemplate, string(value))
				if err != nil {
					data = data + dlm + strValue
				} else {
					if quotas {
						data = data + dlm + "\"" + fmt.Sprintf(field.Format, time.Format(dateTemplate)) + "\""
					} else {
						data = data + dlm + fmt.Sprintf(field.Format, time.Format(dateTemplate))
					}
				}
			case "time":
				data = data + dlm + strValue
			case "int":
				v, err := strconv.ParseInt(string(value), 10, 64)
				if err != nil {
					data = data + dlm + strValue
				} else {
					data = data + dlm + fmt.Sprintf(field.Format, v)
				}
			case "bool":
				v, err := strconv.ParseBool(string(value))
				if err != nil {
					data = data + dlm + strValue
				} else {
					data = data + dlm + fmt.Sprintf(field.Format, v)
				}
			default:
				continue
			}
			data = data + finalizer
		}
	}

	return
}
