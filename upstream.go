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

// replayableStatus reports whether a status is an error a backend explained and
// so can stand as the answer when every backend gave it. Everything from 400 up
// qualifies, because openvoxdb does not reserve 5xx for its own faults: a
// malformed query reaches the client as a 400 only when something on the way
// down happens to throw a class the query engine catches (query_eng.clj:571-583
// catches IllegalArgumentException and the :puppetlabs.puppetdb.query/invalid
// ExceptionInfo), and as a 500 otherwise. ["=","name"] is a 400 on /nodes and a
// 500 with an empty body on /facts, because only the facts entity runs
// rewrite-fact-query, whose name-constraint does an unguarded (nth clause 2)
// (engine.clj:2988-3003); a bare string or number for query is a 500 reading
// "Output of convert-query-params does not match schema" from the schema check
// in front of the parser. Drawing the line at 500 would replay one of those and
// swallow the other, which is the route-dependence clients notice.
//
// Below 400 nothing is replayed. A 3xx carries its meaning in Location, which
// the fan-out does not keep, and a non-200 2xx is not an error at all.
func replayableStatus(status int) bool {
	return status >= 400
}

// unanimousUpstreamError returns the reply to replay when every backend answered
// the same request the same way, and nil otherwise. Every backend is asked the
// same question, so unanimity is what separates the estate's real answer from a
// sick estate: a transport failure, or two backends disagreeing on the status,
// leaves at least one backend whose answer is evidence about the backend rather
// than about the request, and that is what 502 describes.
//
// A nil entry stands for a backend that did not answer with an HTTP status at
// all, which defeats unanimity.
//
// The reply returned is the first in configured order, so a client retrying a
// rejected query is told the same thing every time.
func unanimousUpstreamError(errs []*upstreamError) *upstreamError {
	var first *upstreamError
	for _, ue := range errs {
		if ue == nil || !replayableStatus(ue.status) {
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

// upstreamOutcome is the error to answer a fan-out with when it produced
// nothing: the backends' own unanimous reply where there is one, and pdbmux's
// gateway error otherwise. Every handler ends here, so no route answers a
// failure differently from any other.
func upstreamOutcome(errs []*upstreamError) error {
	if ue := unanimousUpstreamError(errs); ue != nil {
		return ue
	}
	return errAllBackendsFailed
}

// backendUpstreamErrors reduces a merged fan-out's results to one entry per
// backend, nil where the backend answered or failed without a status.
func backendUpstreamErrors(results []backendResult) []*upstreamError {
	errs := make([]*upstreamError, len(results))
	for i, res := range results {
		var ue *upstreamError
		if errors.As(res.err, &ue) {
			errs[i] = ue
		}
	}
	return errs
}

// clientRefusal reports whether err is a unanimous upstream rejection blaming
// the request. Only those mean the estate is healthy and the query was wrong,
// which is why they are neither counted as degraded service nor answered from
// the cache; a unanimous 5xx is replayed just the same but is the backends
// reporting their own fault.
func clientRefusal(err error) bool {
	var ue *upstreamError
	return errors.As(err, &ue) && ue != nil && ue.status < 500
}

// writeUpstreamError answers a fan-out that produced no records. A unanimous
// upstream error is replayed with the backend's own status, content type and
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
		_, _ = fmt.Fprintf(w, "every backend answered %d %s\n", ue.status, http.StatusText(ue.status))
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
