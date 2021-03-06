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
	specification Fields, idName, atName string, renderMap *RenderMap,
	columns []string, values []interface{},
) {
	for _, cursor := range specification {
		// Check cursor for ID field
		if strings.ToLower(cursor.Topic) == strings.ToLower(idName) ||
			(cursor.Topic == "" && strings.ToLower(cursor.Name) == strings.ToLower(idName)) {
			c.ID = renderMap.Render(cursor, columns, values)
		}
		// Check cursor for AT field
		if strings.ToLower(cursor.Topic) == strings.ToLower(atName) ||
			(cursor.Topic == "" && strings.ToLower(cursor.Name) == strings.ToLower(atName)) {
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
	DateFormat   string
	TimeTemplate string
	TimeFormat   string
	Delimiter    string
	Finalizer    string
	UseNames     bool
	Quotas       bool
	Extras       bool
}

// Renderer describes Render method
type Renderer interface {
	Render(field Field, columns []string, values []interface{}) (data string)
}

// Render generates data
func (r *RenderMap) Render(field Field, columns []string, values []interface{}) (data string) {
	var extrasFound bool
	for ndx, name := range columns {
		if field.Name != "" && field.Name != name {
			continue
		}
		if r.Extras {
			extrasFound = true
			if field.Topic == "" {
				continue
			}
			name = field.Topic
		}
		if v, ok := values[ndx].([]byte); ok {
			data = data + r.parse(string(v), name, field.Type, field.Format)
		}
	}
	if r.Extras && !extrasFound {
		data = data + r.parse(field.Name, field.Topic, field.Type, field.Format)
	}

	return
}

func (r *RenderMap) parse(value, name, fieldType, format string) (data string) {
	strValue := fmt.Sprintf(format, value)
	dlm := r.Delimiter
	if r.Quotas {
		strValue = "\"" + strings.NewReplacer(
			"\"", "\\\"", "\\", "\\\\", "\n", "", "\r", "",
		).Replace(fmt.Sprintf(format, value)) + "\""
	}
	if r.UseNames {
		if r.Quotas {
			dlm = "\"" + name + "\"" + r.Delimiter
		} else {
			dlm = name + r.Delimiter
		}
	}
	switch fieldType {
	case "string":
		data = data + dlm + strValue
	case "date":
		time, err := time.Parse(r.TimeTemplate, string(value))
		if err != nil {
			data = data + dlm + strValue
		} else {
			if r.Quotas {
				data = data + dlm + "\"" + fmt.Sprintf(format, time.Format(r.DateFormat)) + "\""
			} else {
				data = data + dlm + fmt.Sprintf(format, time.Format(r.DateFormat))
			}
		}
	case "time":
		time, err := time.Parse(r.TimeTemplate, string(value))
		if err != nil {
			data = data + dlm + strValue
		} else {
			if r.Quotas {
				data = data + dlm + "\"" + fmt.Sprintf(format, time.Format(r.TimeFormat)) + "\""
			} else {
				data = data + dlm + fmt.Sprintf(format, time.Format(r.TimeFormat))
			}
		}
	case "int":
		v, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			data = data + dlm + strValue
		} else {
			data = data + dlm + fmt.Sprintf(format, v)
		}
	case "float":
		v, err := strconv.ParseFloat(string(value), 32)
		if err != nil {
			data = data + dlm + strValue
		} else {
			data = data + dlm + fmt.Sprintf(format, v)
		}
	case "bool":
		v, err := strconv.ParseBool(string(value))
		if err != nil {
			data = data + dlm + strValue
		} else {
			data = data + dlm + fmt.Sprintf(format, v)
		}
	default:
		return
	}
	data = data + r.Finalizer

	return
}

// Skipped checks data for skipped records
func Skipped(rendermap *RenderMap, include, exclude Fields, columns []string, values []interface{}) bool {
	// Check filters
	if len(include) == 0 {
		for _, field := range exclude {
			if field.Topic == rendermap.Render(field, columns, values) {
				return true
			}
		}
	} else {
		for _, field := range include {
			if field.Topic == rendermap.Render(field, columns, values) {
				return false
			}
		}

		return true
	}

	return false

}
