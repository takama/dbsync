package file

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// File driver
type File struct {
	dataDir string
	id      string
	topics  []string
	spec    Fields
	path    Fields
	name    Fields
	header  Fields
	columns Fields
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for file syncing")

// New creates file driver
func New(
	dataDir, id string, topics []string,
	spec, path, name, header, columns Fields,
) (db *File, err error) {
	db = &File{
		dataDir: dataDir,
		id:      id,
		topics:  topics,
		spec:    spec,
		path:    path,
		name:    name,
		header:  header,
		columns: columns,
	}
	err = db.checkDatastorePath()
	return
}

// LastID implements interface for getting last ID in datastore bucket
func (db *File) LastID(bucket string) (id uint64, err error) {
	return db.lastID(bucket)
}

// AddFromSQL implements interface for inserting data from SQL into bucket
func (db *File) AddFromSQL(bucket string, columns []string, values []interface{}) (last uint64, err error) {
	// Get last ID
	for ndx, name := range columns {
		if name == db.id {
			if value, ok := values[ndx].([]byte); ok {
				last, err = strconv.ParseUint(string(value), 10, 64)
				if err != nil {
					return
				}
			}
		}
	}
	for _, topic := range db.topics {

		// Generate path
		path := topic
		for _, field := range db.path {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			path = path + db.generateData(field, string(os.PathSeparator), "", false, columns, values)
		}

		// Generate name
		for _, field := range db.name {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			path = path + db.generateData(field, string(os.PathSeparator), "", false, columns, values)
		}

		// Generate header
		data := "\n"
		for _, field := range db.header {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			data = data + db.generateData(field, " ", "", false, columns, values)
		}
		data = data + "\n" + strings.Repeat("=", len(data)) + "\n"

		// Generate data columns
		if len(db.columns) > 0 {
			for _, field := range db.columns {
				if field.Topic != "" && field.Topic != topic {
					continue
				}
				data = data + db.generateData(field, ": ", "\n", true, columns, values)
			}
		} else {
			data = data + db.generateData(
				Field{Type: "string", Format: "%s"},
				": ", "\n", true, columns, values,
			)
		}

		// Save data
		err = db.save(bucket, path, data)
		if err != nil {
			return 0, err
		}
	}

	at := "0000-00-00"
	// Check spec for AT field
	for _, spec := range db.spec {
		if strings.ToLower(spec.Topic) == "at" ||
			(spec.Topic == "" && strings.ToLower(spec.Name) == "at") {
			at = db.generateData(spec, "", "", false, columns, values)
		}
	}
	// Save Last ID and AT
	err = db.saveLastID(bucket, last, at)
	if err != nil {
		return 0, err
	}
	return
}

func (db *File) generateData(
	field Field, delimiter, finalizer string, useNames bool,
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
			if useNames {
				dlm = name + delimiter
			}
			switch field.Type {
			case "string":
				data = data + dlm + fmt.Sprintf(field.Format, string(value)) + finalizer
			case "date":
				time, err := time.Parse(timeTemplate, string(value))
				if err != nil {
					data = data + dlm + fmt.Sprintf(field.Format, string(value))
				} else {
					data = data + dlm + fmt.Sprintf(field.Format, time.Format(dateTemplate))
				}
				data = data + finalizer
			case "time":
				data = data + dlm + fmt.Sprintf(field.Format, string(value)) + finalizer
			}
		}
	}

	return
}

func (db *File) bucketName(bucket string) string {
	r := strings.NewReplacer("-", "", "_", "", " ", "")
	return r.Replace(bucket)
}

func (db *File) save(bucket, path, data string) error {
	path = db.dataDir + string(os.PathSeparator) + db.bucketName(bucket) + string(os.PathSeparator) + path
	dir := filepath.Dir(path)
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(data)
	return err
}

func (db *File) saveLastID(bucket string, id uint64, at string) error {
	path := db.dataDir + string(os.PathSeparator) + db.bucketName(bucket) + string(os.PathSeparator) + "lastID"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	data := struct {
		ID uint64
		AT string
	}{ID: id, AT: at}
	fileWriter := bufio.NewWriter(file)
	err = json.NewEncoder(fileWriter).Encode(data)
	if err == nil {
		fileWriter.Flush()
	}
	return err
}

func (db *File) lastID(bucket string) (id uint64, err error) {
	path := db.dataDir + string(os.PathSeparator) + db.bucketName(bucket)
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		if err := os.Mkdir(path, os.ModeDir|0755); err != nil {
			return 0, err
		}
	}
	idPath := path + string(os.PathSeparator) + "lastID"
	_, err = os.Stat(idPath)
	// if file does not exist, return "0" without error
	if os.IsNotExist(err) {
		return 0, nil
	}
	file, err := os.OpenFile(idPath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	var data struct {
		ID uint64
		AT string
	}
	err = json.NewDecoder(bufio.NewReader(file)).Decode(&data)
	if err != nil {
		return 0, err
	}
	return data.ID, nil
}

// checkDatastorePath - checks if not exists datastore file try to create it
func (db *File) checkDatastorePath() error {
	_, err := os.Stat(db.dataDir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(db.dataDir, os.ModeDir|0755); err != nil {
			return err
		}
	}
	chkFile := db.dataDir + string(os.PathSeparator) + "store.chk"
	file, err := os.OpenFile(chkFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString("ok")
	if err != nil {
		return err
	}

	return nil
}
