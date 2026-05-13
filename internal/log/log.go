package log

import (
	"fmt"
	"io"
	"log/syslog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
)

type Logger = zerolog.Logger

type ReopenableWriter struct {
	fh atomic.Pointer[os.File]
}

func (w *ReopenableWriter) Write(p []byte) (int, error) {
	return w.fh.Load().Write(p)
}

func (w *ReopenableWriter) reopen(path string) error {
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open log %s: %w", path, err)
	}
	old := w.fh.Swap(fh)
	if old != nil && old != os.Stderr {
		_ = old.Close()
	}
	return nil
}

func parseLevel(level string) (zerolog.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn", "warning":
		return zerolog.WarnLevel, nil
	case "err", "error":
		return zerolog.ErrorLevel, nil
	case "fatal":
		return zerolog.FatalLevel, nil
	}
	return zerolog.InfoLevel, fmt.Errorf("unknown log level %q", level)
}

func levelToUpper(i interface{}) string {
	if i == nil {
		return ""
	}
	return strings.ToUpper(fmt.Sprintf("%-5s", i))
}

func Open(path string, level string, bufSize int, poll time.Duration) (*Logger, io.Closer, *ReopenableWriter, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, nil, nil, err
	}

	var out io.Writer
	var rw *ReopenableWriter

	if path == "" || path == "/dev/stderr" {
		out = os.Stderr
	} else {
		fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to open log %s: %w", path, err)
		}
		rw = &ReopenableWriter{}
		rw.fh.Store(fh)
		out = rw
	}

	cw := zerolog.ConsoleWriter{
		Out:         out,
		NoColor:     true,
		TimeFormat:  time.RFC3339,
		FormatLevel: levelToUpper,
	}
	dw := diode.NewWriter(cw, bufSize, poll, nil)
	l := zerolog.New(dw).Level(lvl).With().Timestamp().Logger()
	return &l, dw, rw, nil
}

// ReOpenOnSignal reopens the logfile on signal.
func ReOpenOnSignal(sig syscall.Signal, rw *ReopenableWriter, path string, sl *syslog.Writer) {
	if rw == nil {
		return
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, sig)
	go func() {
		for {
			<-sigs
			if err := rw.reopen(path); err != nil {
				WriteSysLogError(sl, fmt.Sprintf("failed to reopen log file: %v", err))
			}
		}
	}()
}
