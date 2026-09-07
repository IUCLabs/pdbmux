package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// upstreamBodyLimit caps how much of a backend's error body is kept for replay.
const upstreamBodyLimit = 8 << 10

// redactedBackend stands in for anything naming a backend in a replayed body.
const redactedBackend = "<backend>"

// upstreamError is a non-2xx reply from a backend kept whole — status, content
// type and body — so a rejection the backend explained can be replayed to the
// client instead of collapsed into a gateway error that throws the explanation
// away.
type upstreamError struct {
	status      int
	contentType string
	body        []byte
}

func newUpstreamError(status int, contentType string, body []byte) *upstreamError {
	if len(body) > upstreamBodyLimit {
		body = body[:upstreamBodyLimit]
	}
	return &upstreamError{status: status, contentType: contentType, body: body}
}

func (e *upstreamError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.status, strings.TrimSpace(string(e.body)))
}

// clientShaped reports whether a status blames the request rather than the
// backend or pdbmux itself. openvoxdb answers every bad query with 400 —
// src/puppetlabs/puppetdb/middleware.clj:98-116 and http.clj:115-129 — and its
// other 4xx say something a client of pdbmux neither caused nor can fix: 403 is
// pdbmux's own certificate being refused (middleware.clj:44-58); 404 is either
// an absent object or an unknown path (http.clj:238-242,
// middleware.clj:381-398), and which objects a backend holds is the one thing
// backends are meant to disagree about; 408 and 429 report a backend's timing
// and capacity. A backend that times out mid-query answers 200 with a truncated
// body rather than any 4xx (query_eng.clj:463-483), so no timeout reaches here.
//
// Every 4xx left over blames the request and so is safe to replay, though none
// is reachable while the fan-out forwards no client header: 406 needs an Accept
// the query app refuses (http/server.clj:72; must-accept-type in
// http.clj:136-145 is unwired), and 415 a Content-Encoding on a POST to
// /commands (middleware.clj:165-178), which pdbmux never proxies.
func clientShaped(status int) bool {
	switch status {
	case http.StatusForbidden, http.StatusNotFound,
		http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return status >= 400 && status < 500
}

// unanimousClientError returns the rejection to replay when every backend in a
// fan-out refused the same query with the same client-shaped status, and nil
// otherwise. Every backend is asked the same question, so unanimity is what
// distinguishes a bad query from a sick estate: a transport failure, a 5xx, or
// two backends disagreeing on the status all leave at least one backend whose
// answer is evidence about the backend rather than about the request.
//
// The reply returned is the first in configured order, so a client retrying a
// rejected query is told the same thing every time.
func unanimousClientError(results []backendResult) *upstreamError {
	var first *upstreamError
	for _, res := range results {
		var ue *upstreamError
		if !errors.As(res.err, &ue) || !clientShaped(ue.status) {
			return nil
		}
		if first == nil {
			first = ue
		} else if ue.status != first.status {
			return nil
		}
	}
	return first
}

// writeUpstreamError answers a fan-out that produced no records. A unanimous
// client-shaped rejection is replayed with the backend's own status and
// explanation; anything else is reported as a gateway failure.
func (s *Server) writeUpstreamError(w http.ResponseWriter, err error) {
	var ue *upstreamError
	if !errors.As(err, &ue) {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	body := s.redactBackends(ue.body)
	if len(strings.TrimSpace(string(body))) == 0 {
		setContentType(w, "text/plain; charset=utf-8")
		w.WriteHeader(ue.status)
		_, _ = fmt.Fprintf(w, "upstream rejected the query: %s\n", http.StatusText(ue.status))
		return
	}
	setContentType(w, ue.contentType)
	w.WriteHeader(ue.status)
	_, _ = w.Write(body)
}

// redactBackends strips anything naming a configured backend out of text bound
// for a client. Hiding the estate behind one endpoint is the point of pdbmux, so
// a backend's own words may reach the client but its address may not.
func (s *Server) redactBackends(body []byte) []byte {
	out := string(body)
	for _, b := range s.cfg.Backends {
		for _, term := range backendTerms(b) {
			out = replaceFold(out, term, redactedBackend)
		}
	}
	return []byte(out)
}

// replaceFold is strings.ReplaceAll ignoring ASCII case, because DNS names are
// case-insensitive and a backend shouted back in capitals is still named.
func replaceFold(s, old, repl string) string {
	if old == "" {
		return s
	}
	hay, needle := foldASCII(s), foldASCII(old)
	var out strings.Builder
	for {
		i := strings.Index(hay, needle)
		if i < 0 {
			out.WriteString(s)
			return out.String()
		}
		out.WriteString(s[:i])
		out.WriteString(repl)
		s, hay = s[i+len(needle):], hay[i+len(needle):]
	}
}

// foldASCII lowercases ASCII only, so byte offsets into the result also index
// the input — which Unicode-aware folding does not guarantee.
func foldASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

// backendTerms is what identifies a backend in text: its URL, that URL's host
// and the bare hostname, longest first so replacing one cannot leave a fragment
// of another behind. The configured name is deliberately absent — no backend
// knows it, and names are short enough to match ordinary words.
func backendTerms(b Backend) []string {
	terms := []string{}
	if b.URL != "" {
		terms = append(terms, strings.TrimRight(b.URL, "/"))
	}
	u, err := url.Parse(b.URL)
	if err != nil || u.Host == "" {
		return terms
	}
	terms = append(terms, u.Host)
	if hn := u.Hostname(); hn != u.Host {
		terms = append(terms, hn)
	}
	return terms
}
