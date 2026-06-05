package hutch

// Logger is the minimal logging surface hutch needs. It is satisfied by the
// standard library's *log.Logger, so you can pass log.Default() directly. The
// default is a no-op.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}
