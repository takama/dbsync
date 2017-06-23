package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"text/template"
	"unicode"

	"github.com/takama/dbsync/pkg/datastore"
	"github.com/takama/dbsync/version"
)

type handler struct {
	stdlog *log.Logger
	errlog *log.Logger
	env    map[string]string

	datastore.DB
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.stdlog.Println(r.Method, r.URL.Path)
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/":
			fmt.Fprintln(w, "[DBSYNC]: use '/status' to view detailed report by the last operations")
		case "/healthz":
			fmt.Fprintln(w, "Ok")
		case "/info":
			h.info(w)
		case "/status":
			h.status(w)
		default:
			writeError(w, http.StatusNotFound)
		}
	default:
		writeError(w, http.StatusMethodNotAllowed)
	}
}

func (h *handler) status(w http.ResponseWriter) {
	statuses := h.DB.Report()
	fmt.Fprint(w, "Tables sync status\n==================\n")
	templ, err := template.New("statusList").Parse(statusList)
	if err == nil {
		templ.Execute(w, statuses)
		return
	}
}

func (h *handler) info(w http.ResponseWriter) {
	js, err := json.MarshalIndent(
		map[string]string{
			"version": version.RELEASE,
			"commit":  version.COMMIT,
			"repo":    version.REPO,
		},
		"",
		"  ",
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func writeError(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}

// Setup server with env and handler
func Setup() (srv http.Server, err error) {
	keys := []string{
		"DBSYNC_SERVICE_PORT", "DBSYNC_START_AFTER_ID",
		"DBSYNC_DST_DB_TABLES_PREFIX", "DBSYNC_DST_DB_TABLES_POSTFIX",
		"DBSYNC_UPDATE_TABLES", "DBSYNC_INSERT_TABLES",
		"DBSYNC_UPDATE_PERIOD", "DBSYNC_INSERT_PERIOD",
		"DBSYNC_UPDATE_ROWS", "DBSYNC_INSERT_ROWS",
		"DBSYNC_SRC_DB_DRIVER", "DBSYNC_SRC_DB_HOST", "DBSYNC_SRC_DB_PORT",
		"DBSYNC_SRC_DB_NAME", "DBSYNC_SRC_DB_USERNAME", "DBSYNC_SRC_DB_PASSWORD",
		"DBSYNC_DST_DB_DRIVER", "DBSYNC_DST_DB_HOST", "DBSYNC_DST_DB_PORT",
		"DBSYNC_DST_DB_NAME", "DBSYNC_DST_DB_USERNAME", "DBSYNC_DST_DB_PASSWORD",
	}
	h := &handler{
		stdlog: log.New(os.Stdout, "[DBSYNC:INFO]: ", log.LstdFlags),
		errlog: log.New(os.Stderr, "[DBSYNC:ERROR]: ", log.Ldate|log.Ltime|log.Lshortfile),
		env:    make(map[string]string, len(keys)),
	}
	for _, key := range keys {
		value := os.Getenv(key)
		if value == "" && key != "DBSYNC_UPDATE_TABLES" &&
			key != "DBSYNC_INSERT_TABLES" && key != "DBSYNC_DST_DB_TABLES_PREFIX" &&
			key != "DBSYNC_DST_DB_TABLES_POSTFIX" {
			err = fmt.Errorf("%s environment variable was not set", key)
			return
		}
		h.env[key] = value
	}
	splitf := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c) && c != '_' && c != '-'
	}
	srcPort, err := strconv.ParseUint(h.env["DBSYNC_SRC_DB_PORT"], 10, 64)
	if err != nil {
		return
	}
	dstPort, err := strconv.ParseUint(h.env["DBSYNC_DST_DB_PORT"], 10, 64)
	if err != nil {
		return
	}
	updatePeriod, err := strconv.ParseUint(h.env["DBSYNC_UPDATE_PERIOD"], 10, 64)
	if err != nil {
		return
	}
	insertPeriod, err := strconv.ParseUint(h.env["DBSYNC_INSERT_PERIOD"], 10, 64)
	if err != nil {
		return
	}
	updateRows, err := strconv.ParseUint(h.env["DBSYNC_UPDATE_ROWS"], 10, 64)
	if err != nil {
		return
	}
	insertRows, err := strconv.ParseUint(h.env["DBSYNC_INSERT_ROWS"], 10, 64)
	if err != nil {
		return
	}
	startAfterID, err := strconv.ParseUint(h.env["DBSYNC_START_AFTER_ID"], 10, 64)
	if err != nil {
		return
	}
	h.DB, err = datastore.New(
		h.env["DBSYNC_SRC_DB_DRIVER"], h.env["DBSYNC_SRC_DB_HOST"], h.env["DBSYNC_SRC_DB_NAME"],
		h.env["DBSYNC_SRC_DB_USERNAME"], h.env["DBSYNC_SRC_DB_PASSWORD"], srcPort,
		h.env["DBSYNC_DST_DB_DRIVER"], h.env["DBSYNC_DST_DB_HOST"], h.env["DBSYNC_DST_DB_NAME"],
		h.env["DBSYNC_DST_DB_USERNAME"], h.env["DBSYNC_DST_DB_PASSWORD"], dstPort,
		strings.FieldsFunc(h.env["DBSYNC_UPDATE_TABLES"], splitf),
		strings.FieldsFunc(h.env["DBSYNC_INSERT_TABLES"], splitf),
		h.env["DBSYNC_DST_DB_TABLES_PREFIX"], h.env["DBSYNC_DST_DB_TABLES_POSTFIX"],
		updatePeriod, insertPeriod, updateRows, insertRows, startAfterID,
	)
	srv.Addr = ":" + h.env["DBSYNC_SERVICE_PORT"]
	srv.Handler = h
	return
}

var statusList = `
+=============================================================================================================================+
| TABLES                    |    INSERTED |     UPDATED |      ERRORS |     LAST ID | IN PROGRESS |            EXEC/IDLE TIME |
+=============================================================================================================================+
{{ range $v := . }}| {{ printf "%-25s" $v.Table }} | {{ printf "% 11d" $v.Inserted }} | {{ printf "% 11d" $v.Updated }} | {{ printf "% 11d" $v.Errors }} | {{ printf "% 11d" $v.LastID }} | {{ printf "% 11t" $v.Running }} | {{ printf "% 25s" $v.Duration }} |
+---------------------------+-------------+-------------+-------------+-------------+-------------+---------------------------+
{{end}}
`
