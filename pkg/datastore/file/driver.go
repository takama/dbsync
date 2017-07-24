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
	renderMap *mapping.RenderMap

	exclude mapping.Fields
	columns mapping.Fields
	spec    mapping.Fields
	cursors map[string]*mapping.Cursor

	dataDir string

	json        bool
	compression bool
	extension   string
	bucket      string
	topics      []string
	match       string
	path        mapping.Fields
	name        mapping.Fields
	header      mapping.Fields

	flow map[string]binding.Stream
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
	renderMap *mapping.RenderMap, exclude, columns, spec mapping.Fields,
	dataDir string, json, compression bool, extension, bucket string,
	topics []string, match string, path, name, header mapping.Fields,
) (db *File, err error) {
	db = &File{
		renderMap: renderMap,

		exclude: exclude,
		columns: columns,
		spec:    spec,
		cursors: make(map[string]*mapping.Cursor),

		dataDir: dataDir,

		json:        json,
		compression: compression,
		extension:   extension,
		bucket:      bucket,
		topics:      topics,
		match:       match,
		path:        path,
		name:        name,
		header:      header,

		flow: make(map[string]binding.Stream),
	}
	err = db.checkDatastorePath()
	return
}

// LastID implements interface for getting last ID in datastore bucket
func (db *File) LastID(bucket string) (id uint64, err error) {
	cursor, err := db.getCursor(bucket)
	if err != nil {
		return
	}
	v, err := strconv.ParseUint(string(cursor.ID), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Cursor sets pointer
func (db *File) Cursor(
	bucket string, renderMap *mapping.RenderMap, columns []string, values []interface{},
) (last uint64, err error) {
	// Get and decode cursor
	cursor, err := db.getCursor(bucket)
	if err != nil {
		return
	}
	cursor.Decode(db.spec, renderMap, columns, values)

	// Save cursor
	db.cursors[bucket] = cursor
	last, err = strconv.ParseUint(cursor.ID, 10, 64)
	if err != nil {
		return
	}
	if last == 0 {
		return last, ErrEmptyID
	}

	return
}

// AddFromSQL implements interface for inserting data from SQL into bucket
func (db *File) AddFromSQL(bucket string, columns []string, values []interface{}) (last uint64, err error) {
	// Get last ID
	last, err = db.Cursor(bucket, db.renderMap, columns, values)
	if err != nil {
		return
	}
	for _, topic := range db.topics {

		renderMap := new(mapping.RenderMap)
		*renderMap = *db.renderMap
		renderMap.Delimiter = string(os.PathSeparator)

		// Generate path
		path := topic
		for _, field := range db.path {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			part := renderMap.Render(field, columns, values)
			if part == string(os.PathSeparator) || part == "" {
				return last, ErrInvalidPath
			}
			path = path + part
		}

		// Generate name
		for _, field := range db.name {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			path = path + renderMap.Render(field, columns, values)
		}
		if str := strings.Trim(db.extension, ". "); str != "" {
			path = path + "." + str
		}

		// Generate header
		data := "\n"
		renderMap.Delimiter = " "
		for _, field := range db.header {
			if field.Topic != "" && field.Topic != topic {
				continue
			}
			data = data + renderMap.Render(field, columns, values)
		}
		data = data + "\n" + strings.Repeat("=", len(data)) + "\n"

		// Generate data columns
		listMap := new(mapping.RenderMap)
		*listMap = *db.renderMap
		listMap.Delimiter = ": "
		listMap.Finalizer = "\n"
		listMap.UseNames = true
		if len(db.columns) > 0 {
			if db.json {
				jsonMap := new(mapping.RenderMap)
				*jsonMap = *db.renderMap
				jsonMap.Delimiter = ": "
				jsonMap.Finalizer = ", "
				jsonMap.UseNames = true
				jsonMap.Quotas = true
				data = data + "{"
				for _, field := range db.columns {
					if field.Topic != "" && field.Topic != topic {
						continue
					}
					data = data + jsonMap.Render(field, columns, values)
				}
				data = strings.Trim(data, ", ") + "}"
			} else {
				for _, field := range db.columns {
					if field.Topic != "" && field.Topic != topic {
						continue
					}
					data = data + listMap.Render(field, columns, values)
				}
			}
		} else {
			data = data + listMap.Render(
				mapping.Field{Type: "string", Format: "%s"}, columns, values,
			)
		}

		// Save data
		err = db.save(bucket, path, data)
		if err != nil {
			return
		}
	}

	return
}

// Close flushes data and closes files
func (db *File) Close() (err error) {
	err = binding.Close(db.flow)
	if err != nil {
		return
	}
	for bucket := range db.cursors {
		err = db.saveCursor(bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetFiles should collect
func (db *File) GetFiles(path string, fileCount int) (collection map[string]binding.Stream, err error) {
	cursor, err := db.getCursor(db.bucket)
	if err != nil {
		return
	}
	collection = make(map[string]binding.Stream)
	var excludes []string
	var errSkip = fmt.Errorf("Skip over files")
	for _, f := range db.exclude {
		if strings.ToLower(f.Name) == "id" {
			excludes = append(excludes, fmt.Sprintf(f.Format, cursor.ID))
		}
		if strings.ToLower(f.Name) == "at" {
			excludes = append(excludes, fmt.Sprintf(f.Format, cursor.AT))
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
		partExtension := ".part"
		if str := strings.Trim(db.extension, ". "); str != "" {
			path = path + "." + str + partExtension
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
		if err := file.Close(); err != nil {
			return err
		}
		if err := os.Rename(path, strings.TrimSuffix(path, partExtension)); err != nil {
			return err
		}
		return stream.Handle.Close()
	}

	return nil
}

// Remove method removes file by path
func (db *File) Remove(path string) error {
	return os.Remove(path)
}

func (db *File) getCursor(bucket string) (cursor *mapping.Cursor, err error) {
	cursor, ok := db.cursors[bucket]
	if ok {
		return
	}
	cursor = &mapping.Cursor{
		ID: "0",
		AT: "0000-00-00",
	}
	path := db.dataDir + string(os.PathSeparator) + db.bucketName(bucket)
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		if err = os.Mkdir(path, os.ModeDir|0755); err != nil {
			return
		}
	}
	idPath := path + string(os.PathSeparator) + "cursor"
	_, err = os.Stat(idPath)
	// if file does not exist, return "0" without error
	if os.IsNotExist(err) {
		return cursor, nil
	}
	file, err := os.OpenFile(idPath, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()
	err = json.NewDecoder(bufio.NewReader(file)).Decode(cursor)
	if err != nil {
		return
	}

	db.cursors[bucket] = cursor
	return
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
	stream, ok := db.flow[path]
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
		if db.compression {
			stream = binding.Stream{
				Handle:   file,
				GZWriter: gzip.NewWriter(file),
			}
		} else {
			stream = binding.Stream{
				Handle: file,
				Writer: bufio.NewWriter(file),
			}
		}
		db.flow[path] = stream
	}
	if db.compression {
		_, err := stream.GZWriter.Write([]byte(data))
		if err != nil {
			stream.Handle.Close()
			delete(db.flow, path)
		}
	} else {
		_, err := stream.Writer.WriteString(data)
		if err != nil {
			stream.Handle.Close()
			delete(db.flow, path)
		}
	}
	return nil
}

func (db *File) saveCursor(bucket string) error {
	path := db.dataDir + string(os.PathSeparator) + db.bucketName(bucket) + string(os.PathSeparator) + "cursor"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	fileWriter := bufio.NewWriter(file)
	err = json.NewEncoder(fileWriter).Encode(db.cursors[bucket])
	if err == nil {
		fileWriter.Flush()
	}
	return err
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
