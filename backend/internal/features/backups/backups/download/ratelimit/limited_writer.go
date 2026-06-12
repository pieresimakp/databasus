package ratelimit

import "io"

// LimitedWriter throttles a server-generated stream (the restore tar is produced
// on the fly into the HTTP response, so there is no source reader to wrap — the
// limiter has to sit on the write side instead).
type LimitedWriter struct {
	writer      io.Writer
	rateLimiter *Limiter
}

func NewLimitedWriter(writer io.Writer, limiter *Limiter) *LimitedWriter {
	return &LimitedWriter{
		writer:      writer,
		rateLimiter: limiter,
	}
}

func (w *LimitedWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.rateLimiter.Wait(int64(len(p)))
	}

	return w.writer.Write(p)
}
