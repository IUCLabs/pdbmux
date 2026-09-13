package main

import (
	"context"
	"net/http"
	"strings"
)

const (
	factsetsPath = "/pdb/query/v4/factsets"
	catalogsPath = "/pdb/query/v4/catalogs"

	certnameRouteName = "/pdb/query/v4/{nodes,factsets,catalogs}/<certname>"
)

// certnamePrefixes are the endpoints whose next path segment is a certname.
// openvoxdb resolves that segment to a parent row before it serves anything
// under it — status-not-found-response for the singleton
// (src/puppetlabs/puppetdb/http/handlers.clj:98-120,252-253,342-343,372 and
// http.clj:238-242) and parent-check for every child path
// (middleware.clj:381-398, mounted at handlers.clj:345-347,255-262,373-380) —
// so each of these paths describes exactly one node's data and 404s when the
// backend holds none of it.
var certnamePrefixes = []string{nodesPath + "/", factsetsPath + "/", catalogsPath + "/"}

// certnameFor returns the certname a path is keyed on, empty when the path is
// not one of these routes.
func certnameFor(path string) string {
	for _, prefix := range certnamePrefixes {
		rest, ok := strings.CutPrefix(path, prefix)
		if !ok {
			continue
		}
		certname, _, _ := strings.Cut(rest, "/")
		return certname
	}
	return ""
}

func isCertnameRoute(path string) bool { return certnameFor(path) != "" }

// serveByOwner answers a path keyed on one certname from the backend that owns
// that certname, so a node resolves here exactly as it resolves in the merged
// collections. Without it the answer is whichever backend is configured first,
// and a node both backends hold reports one backend's facts through /facts and
// the other's through /nodes/<certname>/facts.
//
// The owner's reply is passed through whole rather than merged: openvoxdb
// answers these paths with a bare object or a parent-check 404, neither of
// which is the record array the merges are built on. The remaining backends are
// tried after it, so a node only one backend holds is still served and a
// certname no backend holds still answers with openvoxdb's own 404 body.
//
// askOwnerFirst is what makes the owner's reply the answer even when it is an
// error. A backend that does not hold the certname 404s to say so, so it has no
// opinion to weigh against the owner's.
func (s *Server) serveByOwner(w http.ResponseWriter, r *http.Request) {
	s.proxyOrdered(w, r, s.ownerFirst(r.Context(), certnameFor(r.URL.Path)), askOwnerFirst)
}

// ownerFirst puts the backend holding a certname's newest report ahead of the
// rest, from the same freshness map the /facts merge attributes records with.
// Static merge has no per-certname owner, and neither has a certname absent
// from the map, so both keep configured order — the tie-break the merges use.
func (s *Server) ownerFirst(ctx context.Context, certname string) []Backend {
	if s.cfg.Merge == mergeStatic || certname == "" {
		return s.cfg.Backends
	}
	owner := s.freshnessMap(ctx, nil)[certname]
	if owner == "" {
		return s.cfg.Backends
	}
	ordered := make([]Backend, 0, len(s.cfg.Backends))
	for _, b := range s.cfg.Backends {
		if b.Name == owner {
			ordered = append(ordered, b)
		}
	}
	for _, b := range s.cfg.Backends {
		if b.Name != owner {
			ordered = append(ordered, b)
		}
	}
	return ordered
}
