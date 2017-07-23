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

// Cursor contains current pointer to document
type Cursor struct {
	ID string
	AT string
}

// Decode updates pointers or markers from data
func (c *Cursor) Decode(
	specification Fields, renderMap *RenderMap, columns []string, values []interface{},
) {
	for _, cursor := range specification {
		// Check cursor for ID field
		if strings.ToLower(cursor.Topic) == "id" ||
			(cursor.Topic == "" && strings.ToLower(cursor.Name) == "id") {
			c.ID = renderMap.Render(cursor, columns, values)
		}
		// Check cursor for AT field
		if strings.ToLower(cursor.Topic) == "at" ||
			(cursor.Topic == "" && strings.ToLower(cursor.Name) == "at") {
			c.AT = renderMap.Render(cursor, columns, values)
		}
	}
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

// RenderMap setup renderer
type RenderMap struct {
	DateTemplate string
	TimeTemplate string
	Delimiter    string
	Finalizer    string
	UseNames     bool
	Quotas       bool
}

// Renderer describes Render method
type Renderer interface {
	Render(field Field, columns []string, values []interface{}) (data string)
}

// Render generates data
func (r *RenderMap) Render(field Field, columns []string, values []interface{}) (data string) {
	dlm := r.Delimiter
	for ndx, name := range columns {
		if field.Name != "" && field.Name != name {
			continue
		}
		if value, ok := values[ndx].([]byte); ok {
			strValue := fmt.Sprintf(field.Format, string(value))
			if r.Quotas {
				strValue = "\"" + strings.NewReplacer(
					"\"", "\\\"", "\\", "\\\\", "\n", "", "\r", "",
				).Replace(fmt.Sprintf(field.Format, string(value))) + "\""
			}
			if r.UseNames {
				if r.Quotas {
					dlm = "\"" + name + "\"" + r.Delimiter
				} else {
					dlm = name + r.Delimiter
				}
			}
			switch field.Type {
			case "string":
				data = data + dlm + strValue
			case "date":
				time, err := time.Parse(r.TimeTemplate, string(value))
				if err != nil {
					data = data + dlm + strValue
				} else {
					if r.Quotas {
						data = data + dlm + "\"" + fmt.Sprintf(field.Format, time.Format(r.DateTemplate)) + "\""
					} else {
						data = data + dlm + fmt.Sprintf(field.Format, time.Format(r.DateTemplate))
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
			case "float":
				v, err := strconv.ParseFloat(string(value), 32)
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
			data = data + r.Finalizer
		}
	}

	return
}
