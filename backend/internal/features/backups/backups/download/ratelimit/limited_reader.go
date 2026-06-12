package ratelimit

import "io"

type LimitedReader struct {
	reader      io.ReadCloser
	rateLimiter *Limiter
}

func NewLimitedReader(reader io.ReadCloser, limiter *Limiter) *LimitedReader {
	return &LimitedReader{
		reader:      reader,
		rateLimiter: limiter,
	}
}

func (r *LimitedReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if n > 0 {
		r.rateLimiter.Wait(int64(n))
	}
	return n, err
}

func (r *LimitedReader) Close() error {
	return r.reader.Close()
}
