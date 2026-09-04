package executor

import "net/http"

// roundTripperFunc adapts a function to http.RoundTripper for tests.
// Formerly defined alongside the Antigravity REST executor tests; still
// used across executor unit tests.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
