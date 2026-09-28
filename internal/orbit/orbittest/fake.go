// Package orbittest is an in-memory Orbit API for tests: tunnels, the
// hostname sync with the same managed_by rules as the real one, a Shield
// quota, and switches to make it fail.
package orbittest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Site struct {
	Tunnel    int64
	ManagedBy string
	Target    string
	Verified  bool
	Mine      bool // belongs to the token's account
}

type Tunnel struct {
	ID       int64
	Name     string
	LocalURL string
	Token    string
	Pop      int64
}

type Fake struct {
	*httptest.Server
	Mu       sync.Mutex
	Tunnels  map[int64]*Tunnel
	Sites    map[string]*Site
	Quota    int // Shield sites on the account (0 = unlimited)
	Fail     int // answer this many requests with 503 first
	Calls    map[string]int
	Token    string
	next     int64
	tokenSeq int
}

func New(token string) *Fake {
	f := &Fake{Tunnels: map[int64]*Tunnel{}, Sites: map[string]*Site{}, Calls: map[string]int{}, Token: token, next: 1}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

func (f *Fake) SiteCount(mine bool) int {
	n := 0
	for _, s := range f.Sites {
		if s.Mine == mine {
			n++
		}
	}
	return n
}

func (f *Fake) serve(w http.ResponseWriter, r *http.Request) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	key := r.Method + " " + strings.TrimPrefix(r.URL.Path, "/api/v1")
	f.Calls[key]++
	if f.Fail > 0 {
		f.Fail--
		w.WriteHeader(503)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+f.Token {
		writeErr(w, 401, "unauthorized", "bad token")
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	p := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/"), "/")
	switch {
	case r.Method == "GET" && len(p) == 2 && p[1] == "pops":
		writeJSON(w, 200, map[string]any{"available": true, "data": []map[string]any{{"id": 7, "city": "Alpha City"}, {"id": 9, "city": "Beta City"}}})
	case r.Method == "GET" && len(p) == 2 && p[1] == "tunnels":
		var out []map[string]any
		for _, t := range f.sorted() {
			out = append(out, f.rep(t, false))
		}
		writeJSON(w, 200, map[string]any{"data": out})
	case r.Method == "POST" && len(p) == 2 && p[1] == "tunnels":
		name, _ := body["name"].(string)
		for _, t := range f.Tunnels {
			if t.Name == name {
				writeErr(w, 409, "conflict", "exists")
				return
			}
		}
		pop, _ := body["pop_id"].(float64)
		t := &Tunnel{ID: f.next, Name: name, LocalURL: fmt.Sprint(body["local_url"]), Token: f.newToken(), Pop: int64(pop)}
		f.next++
		f.Tunnels[t.ID] = t
		writeJSON(w, 201, f.rep(t, true))
	case len(p) >= 3 && p[1] == "tunnels":
		id, _ := strconv.ParseInt(p[2], 10, 64)
		t := f.Tunnels[id]
		if t == nil {
			writeErr(w, 404, "not_found", "Tunnel not found.")
			return
		}
		switch {
		case len(p) == 3 && r.Method == "GET":
			m := f.rep(t, false)
			m["connect"] = map[string]any{"endpoints": []string{"198.51.100.7:7844"}, "fingerprints": []string{"fp7"}}
			writeJSON(w, 200, m)
		case len(p) == 3 && r.Method == "PATCH":
			if v, ok := body["local_url"].(string); ok {
				t.LocalURL = v
			}
			writeJSON(w, 200, f.rep(t, false))
		case len(p) == 3 && r.Method == "DELETE":
			for h, s := range f.Sites {
				if s.Tunnel == id {
					writeErr(w, 409, "conflict", "This tunnel is the origin for "+h)
					return
				}
			}
			delete(f.Tunnels, id)
			w.WriteHeader(204)
		case len(p) == 4 && p[3] == "token" && r.Method == "POST":
			t.Token = f.newToken()
			writeJSON(w, 200, map[string]any{"token": t.Token, "connect": map[string]any{"endpoints": []string{"198.51.100.7:7844"}, "fingerprints": []string{"fp7"}}})
		case len(p) == 4 && p[3] == "hostnames" && r.Method == "PUT":
			f.sync(w, t, body)
		default:
			writeErr(w, 404, "not_found", "no route")
		}
	default:
		writeErr(w, 404, "not_found", "no route")
	}
}

func (f *Fake) sync(w http.ResponseWriter, t *Tunnel, body map[string]any) {
	mb, _ := body["managed_by"].(string)
	raw, ok := body["hostnames"].([]any)
	if mb == "" || !ok {
		writeErr(w, 400, "validation_error", "bad body")
		return
	}
	adopt, _ := body["adopt"].(bool)
	want := map[string]bool{}
	var out []map[string]any
	for _, x := range raw {
		h := strings.ToLower(fmt.Sprint(x))
		want[h] = true
		s := f.Sites[h]
		switch {
		case s != nil && s.Mine && s.ManagedBy == mb:
			st := "unchanged"
			if s.Tunnel != t.ID {
				s.Tunnel, st = t.ID, "updated"
			}
			out = append(out, map[string]any{"hostname": h, "state": st, "site_id": 1, "target": s.Target, "verified": s.Verified})
		case s != nil && s.Mine && adopt && s.ManagedBy == "":
			s.ManagedBy, s.Tunnel = mb, t.ID
			out = append(out, map[string]any{"hostname": h, "state": "adopted", "site_id": 1, "target": s.Target, "verified": s.Verified})
		case s != nil:
			out = append(out, map[string]any{"hostname": h, "state": "conflict", "site_id": nil, "target": nil, "verified": false, "message": "This hostname is already a Shield site."})
		case f.Quota > 0 && f.SiteCount(true) >= f.Quota:
			out = append(out, map[string]any{"hostname": h, "state": "limit_exceeded", "site_id": nil, "target": nil, "verified": false, "message": "Shield domain limit reached"})
		default:
			tg := strings.ReplaceAll(h, ".", "-") + ".edge.test"
			f.Sites[h] = &Site{Tunnel: t.ID, ManagedBy: mb, Target: tg, Mine: true}
			out = append(out, map[string]any{"hostname": h, "state": "created", "site_id": 1, "target": tg, "verified": false})
		}
	}
	removed := []string{}
	for h, s := range f.Sites {
		if s.Tunnel == t.ID && s.ManagedBy == mb && s.Mine && !want[h] {
			delete(f.Sites, h)
			removed = append(removed, h)
		}
	}
	sort.Strings(removed)
	writeJSON(w, 200, map[string]any{"tunnel_id": t.ID, "managed_by": mb, "hostnames": out, "removed": removed})
}

func (f *Fake) newToken() string { f.tokenSeq++; return fmt.Sprintf("lnk_fake%03d", f.tokenSeq) }

func (f *Fake) sorted() []*Tunnel {
	var ts []*Tunnel
	for _, t := range f.Tunnels {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].ID < ts[j].ID })
	return ts
}

func (f *Fake) rep(t *Tunnel, withToken bool) map[string]any {
	var sites []string
	for h, s := range f.Sites {
		if s.Tunnel == t.ID {
			sites = append(sites, h)
		}
	}
	sort.Strings(sites)
	m := map[string]any{"id": t.ID, "name": t.Name, "local_url": t.LocalURL, "connected": false, "sites": sites}
	if withToken {
		m["token"] = t.Token
		m["connect"] = map[string]any{"endpoints": []string{"198.51.100.7:7844"}, "fingerprints": []string{"fp7"}}
	}
	return m
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, c, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{"code": c, "message": msg}})
}
