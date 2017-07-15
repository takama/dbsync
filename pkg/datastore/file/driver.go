package file

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/takama/dbsync/pkg/datastore/binding"
	"github.com/takama/dbsync/pkg/datastore/mapping"
)

// File driver
type File struct {
	dataDir     string
	json        bool
	compression bool
	bucket      string
	id          string
	topics      []string
	match       string
	exclude     mapping.Fields
	spec        mapping.Fields
	path        mapping.Fields
	name        mapping.Fields
	header      mapping.Fields
	columns     mapping.Fields

	stream  map[string]binding.Stream
	lastIDs map[string]idSpec
}

type idSpec struct {
	ID uint64
	AT string
}

// ErrUnsupported declares error for unsupported methods
var ErrUnsupported = errors.New("Unsupported method for file syncing")

// ErrInvalidPath declares error for invalid path part
var ErrInvalidPath = errors.New("Invalid path data")

// ErrEmptyID declares error for invalid ID
var ErrEmptyID = errors.New("Invalid ID data")

// New creates file driver
func New(
	dataDir, bucket, id string, json, compression bool, topics []string,
	match string, exclude, spec, path, name, header, columns mapping.Fields,
) (db *File, err error) {
	db = &File{
		dataDir:     dataDir,
		json:        json,
		compression: compression,
		bucket:      bucket,
		id:          id,
		topics:      topics,
		match:       match,
		exclude:     exclude,
		spec:        spec,
		path:        path,
		name:        name,
		header:      header,
		columns:     columns,
		stream:      make(map[string]binding.Stream),
		lastIDs:     make(map[string]idSpec),
	}
	err = db.checkDatastorePath()
	return
}

// LastID implements interface for getting last ID in datastore bucket
func (db *File) LastID(bucket string) (id uint64, err error) {
	is, ok := db.lastIDs[bucket]
	if ok {
		return is.ID, nil
	}
	id, at, err := db.lastID(bucket)
	if err != nil {
		return
	}
	db.lastIDs[bucket] = idSpec{ID: id, AT: at}
	return
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
				if last == 0 {
					return last, ErrEmptyID
				}
			}
		}
	}
	// Check filters
	for _, field := range db.exclude {
		if field.Topic == mapping.Render(field, "", "", false, false, columns, values) {
			return
		}
	}
	for _, topic := range db.topics {

		// Generate path
		path := topic
		for _, field := range db.path {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			part := mapping.Render(field, string(os.PathSeparator), "", false, false, columns, values)
			if part == string(os.PathSeparator) {
				return last, ErrInvalidPath
			}
			path = path + part
		}

		// Generate name
		for _, field := range db.name {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			path = path + mapping.Render(field, string(os.PathSeparator), "", false, false, columns, values)
		}

		// Generate header
		data := "\n"
		for _, field := range db.header {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			data = data + mapping.Render(field, " ", "", false, false, columns, values)
		}
		data = data + "\n" + strings.Repeat("=", len(data)) + "\n"

		// Generate data columns
		if len(db.columns) > 0 {
			if db.json {
				data = data + "{"
				for _, field := range db.columns {
					if field.Topic != "" && field.Topic != topic {
						continue
					}
					data = data + mapping.Render(field, ": ", ", ", true, true, columns, values)
				}
				data = strings.Trim(data, ", ") + "}"
			} else {
				for _, field := range db.columns {
					if field.Topic != "" && field.Topic != topic {
						continue
					}
					data = data + mapping.Render(field, ": ", "\n", true, false, columns, values)
				}
			}
		} else {
			data = data + mapping.Render(
				mapping.Field{Type: "string", Format: "%s"},
				": ", "\n", true, false, columns, values,
			)
		}

		// Save data
		err = db.save(bucket, path, data)
		if err != nil {
			return
		}
	}

	at := "0000-00-00"
	// Check spec for AT field
	for _, spec := range db.spec {
		if strings.ToLower(spec.Topic) == "at" ||
			(spec.Topic == "" && strings.ToLower(spec.Name) == "at") {
			at = mapping.Render(spec, "", "", false, false, columns, values)
		}
	}
	// Save Last ID and AT
	db.lastIDs[bucket] = idSpec{ID: last, AT: at}
	return
}

// Close flushes data and closes files
func (db *File) Close() (err error) {
	for ndx, stream := range db.stream {
		err = stream.Writer.Flush()
		if err != nil {
			return err
		}
		err = stream.Handle.Close()
		if err != nil {
			return err
		}
		delete(db.stream, ndx)
	}
	for bucket := range db.lastIDs {
		err = db.saveLastID(bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetFiles should collect
func (db *File) GetFiles(path string, fileCount int) (collection map[string]binding.Stream, err error) {
	id, at, err := db.lastID(db.bucket)
	if err != nil {
		return
	}
	collection = make(map[string]binding.Stream)
	var excludes []string
	var errSkip = fmt.Errorf("Skip over files")
	for _, f := range db.exclude {
		if strings.ToLower(f.Name) == "id" {
			excludes = append(excludes, fmt.Sprintf(f.Format, id))
		}
		if strings.ToLower(f.Name) == "at" {
			excludes = append(excludes, fmt.Sprintf(f.Format, at))
		}
	}
	err = filepath.Walk(
		path,
		func(path string, info os.FileInfo, err error) error {
			// really, we should open every file there?
			if err == nil && !info.IsDir() {
				if match, err := filepath.Match(db.match, filepath.Base(path)); err == nil && !match {
					return nil
				}
				name := strings.TrimPrefix(path, db.dataDir+string(os.PathSeparator)+db.bucket+string(os.PathSeparator))
				for _, ex := range excludes {
					if strings.HasSuffix(name, ex) {
						return nil
					}
				}
				file, err := os.OpenFile(path, os.O_RDONLY, 0644)
				if err != nil {
					return err
				}
				collection[name] = binding.Stream{
					Handle: file,
					Reader: bufio.NewReader(file),
				}
				if len(collection) >= fileCount {
					return errSkip
				}
			}
			return nil
		},
	)
	if err == errSkip {
		return collection, nil
	}

	return
}

// PutFile uploads file to the datastore
func (db *File) PutFile(path string, stream binding.Stream) error {
	// There will be implementation
	if stream.Handle != nil {
		path = db.dataDir + string(os.PathSeparator) + db.bucket + string(os.PathSeparator) + path
		dir := filepath.Dir(path)
		_, err := os.Stat(dir)
		if os.IsNotExist(err) {
			if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
				return err
			}
		}
		if db.compression {
			path = path + ".gz"
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()
		if db.compression {
			w := gzip.NewWriter(file)
			if _, err := io.Copy(w, stream.Reader); err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}
		} else {
			w := bufio.NewWriter(file)
			if _, err := io.Copy(w, stream.Reader); err != nil {
				return err
			}
			if err := w.Flush(); err != nil {
				return err
			}
		}
		return stream.Handle.Close()
	}

	return nil
}

// Remove method removes file by path
func (db *File) Remove(path string) error {
	return os.Remove(path)
}

func (db *File) bucketName(bucket string) string {
	if db.bucket != "" {
		return db.bucket
	}
	r := strings.NewReplacer("-", "", "_", "", " ", "")
	return r.Replace(bucket)
}

func (db *File) save(bucket, path, data string) error {
	path = db.dataDir + string(os.PathSeparator) + db.bucketName(bucket) + string(os.PathSeparator) + path
	stream, ok := db.stream[path]
	if !ok {
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
		stream = binding.Stream{
			Handle: file,
			Writer: bufio.NewWriter(file),
		}
		db.stream[path] = stream
	}
	_, err := stream.Writer.WriteString(data)
	if err != nil {
		stream.Handle.Close()
		delete(db.stream, path)
	}
	return err
}

func (db *File) saveLastID(bucket string) error {
	path := db.dataDir + string(os.PathSeparator) + db.bucketName(bucket) + string(os.PathSeparator) + "lastID"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	fileWriter := bufio.NewWriter(file)
	err = json.NewEncoder(fileWriter).Encode(db.lastIDs[bucket])
	if err == nil {
		fileWriter.Flush()
	}
	return err
}

func (db *File) lastID(bucket string) (id uint64, at string, err error) {
	path := db.dataDir + string(os.PathSeparator) + db.bucketName(bucket)
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		if err := os.Mkdir(path, os.ModeDir|0755); err != nil {
			return 0, "", err
		}
	}
	idPath := path + string(os.PathSeparator) + "lastID"
	_, err = os.Stat(idPath)
	// if file does not exist, return "0" without error
	if os.IsNotExist(err) {
		return 0, "", nil
	}
	file, err := os.OpenFile(idPath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	var data struct {
		ID uint64
		AT string
	}
	err = json.NewDecoder(bufio.NewReader(file)).Decode(&data)
	if err != nil {
		return 0, "", err
	}
	return data.ID, data.AT, nil
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
