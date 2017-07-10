package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"text/template"

	"github.com/takama/dbsync/pkg/datastore"
	"github.com/takama/dbsync/pkg/version"
)

type handler struct {
	stdlog *log.Logger
	errlog *log.Logger

	datastore.DBHandler
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
	statuses := h.DBHandler.Report()
	fmt.Fprint(w, "Tables sync status\n==================\n")
	tmpl, err := template.New("statusList").Parse(statusList)
	if err == nil {
		tmpl.Execute(w, statuses)
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

// Run server with env and handler
func Run() (err error) {
	h := &handler{
		stdlog: log.New(os.Stdout, "[DBSYNC:INFO]: ", log.LstdFlags),
		errlog: log.New(os.Stderr, "[DBSYNC:ERROR]: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
	h.DBHandler, err = datastore.New()
	if err != nil {
		return
	}
	srv := http.Server{
		Addr:    ":" + os.Getenv("DBSYNC_SERVICE_PORT"),
		Handler: h,
	}
	err = h.DBHandler.Run()
	if err != nil {
		return
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			h.errlog.Fatalln(err)
		}
	}()

	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)
	killSignal := <-interrupt
	h.stdlog.Println("Got signal:", killSignal)
	err = h.DBHandler.Shutdown()
	if err != nil {
		h.errlog.Fatalln(err)
	}
	if killSignal == os.Kill {
		h.stdlog.Println("Service was killed")
	} else {
		h.stdlog.Println("Service was terminated by system signal")
	}
	h.stdlog.Println("Gracefully closed")

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
