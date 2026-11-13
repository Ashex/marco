// Command gen writes scan-jetstream's test archive: sealed segments made by
// Jetstream's own segment package (the format a real archive serves), and
// archive.json, which lists every event, and each block's place in its file,
// seq bounds, kinds and collections, so a fake server can plan and serve them.
//
//	cd crates/marco-load/testdata/jsarchive/gen && go run . ..
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
)

const (
	perBlock   = 50
	perSegment = 8
	segments   = 5
	baseUS     = int64(1_760_000_000_000_000)
	follow     = "app.bsky.graph.follow"
	block      = "app.bsky.graph.block"
	listitem   = "app.bsky.graph.listitem"
	like       = "app.bsky.feed.like"
)

type event struct {
	Seq        uint64 `json:"seq"`
	Time       int64  `json:"time"`
	Kind       uint8  `json:"kind"`
	DID        string `json:"did"`
	Collection string `json:"collection,omitempty"`
	Rkey       string `json:"rkey,omitempty"`
	Subject    string `json:"subject,omitempty"`
	List       string `json:"list,omitempty"`
	Active     *bool  `json:"active,omitempty"`
	Status     string `json:"status,omitempty"`
}

type blockInfo struct {
	Offset      uint64   `json:"offset"`
	Size        uint32   `json:"size"`
	MinSeq      uint64   `json:"minSeq"`
	MaxSeq      uint64   `json:"maxSeq"`
	Kinds       []string `json:"kinds"`
	Collections []string `json:"collections"`
}

type segInfo struct {
	Name     string      `json:"name"`
	Checksum string      `json:"checksum"`
	MinSeq   uint64      `json:"minSeq"`
	MaxSeq   uint64      `json:"maxSeq"`
	Blocks   []blockInfo `json:"blocks"`
}

type archive struct {
	Segments []segInfo `json:"segments"`
	Events   []event   `json:"events"`
}

type rng uint64

func (r *rng) below(n int) int {
	*r ^= *r << 13
	*r ^= *r >> 7
	*r ^= *r << 17
	return int(uint64(*r) % uint64(max(n, 1)))
}

const b32 = "234567abcdefghijklmnopqrstuvwxyz"

func tid(us int64, clock int) string {
	v := uint64(us)<<10 | uint64(clock&1023)
	out := make([]byte, 13)
	for i := 12; i >= 0; i-- {
		out[i] = b32[v&31]
		v >>= 5
	}
	return string(out)
}

func did(i int) string {
	if i == 24 {
		return "did:web:web.example"
	}
	v := uint64(i+1) * 0x9e3779b97f4a7c15
	out := make([]byte, 24)
	for j := range out {
		out[j] = b32[v&31]
		v = v>>5 | v<<59
		v ^= uint64(j) * 0x5bd1e995
	}
	return "did:plc:" + string(out)
}

type rec struct{ coll, rkey, subject, list string }

type gen struct {
	r       rng
	seq     uint64
	clock   int
	events  []segment.Event
	out     []event
	live    map[string]map[string]rec
	actors  []string
	created int
}

func (g *gen) next() (uint64, int64) {
	g.seq++
	if len(g.out)%37 == 36 {
		g.seq += 3
	}
	return g.seq, baseUS + int64(g.seq)*1000
}

func record(coll, subject, list string) []byte {
	keys := []string{"$type", "createdAt", "subject"}
	if list != "" {
		keys = append(keys, "list")
	}
	sort.Slice(keys, func(i, j int) bool { return cbor.CompareCBORKeys(keys[i], keys[j]) < 0 })
	b := cbor.AppendMapHeader(nil, uint64(len(keys)))
	for _, k := range keys {
		b = cbor.AppendTextKey(b, k)
		switch k {
		case "$type":
			b = cbor.AppendText(b, coll)
		case "createdAt":
			b = cbor.AppendText(b, "2026-01-01T00:00:00.000Z")
		case "subject":
			b = cbor.AppendText(b, subject)
		case "list":
			b = cbor.AppendText(b, list)
		}
	}
	return b
}

func stamp(us int64) string { return time.UnixMicro(us).UTC().Format("2006-01-02T15:04:05.000000Z") }

func (g *gen) push(ev segment.Event, e event) {
	ev.Seq, ev.WitnessedAt = g.next()
	e.Seq, e.Time = ev.Seq, ev.WitnessedAt
	if ev.Rev == "" {
		ev.Rev = tid(ev.WitnessedAt, 0)
	}
	g.events = append(g.events, ev)
	g.out = append(g.out, e)
}

func (g *gen) commit(kind segment.Kind, d string, r rec) {
	var payload []byte
	if kind != segment.KindDelete {
		payload = record(r.coll, r.subject, r.list)
	}
	g.push(segment.Event{Kind: kind, DID: d, Collection: r.coll, Rkey: r.rkey, Payload: payload},
		event{Kind: uint8(kind), DID: d, Collection: r.coll, Rkey: r.rkey, Subject: r.subject, List: r.list})
	if r.coll == like {
		return
	}
	if g.live[d] == nil {
		g.live[d] = map[string]rec{}
	}
	k := r.coll + "/" + r.rkey
	if kind == segment.KindDelete {
		delete(g.live[d], k)
	} else {
		g.live[d][k] = r
	}
}

func (g *gen) rkey() string {
	g.clock++
	return tid(baseUS+int64(g.seq)*1000, g.clock)
}

func (g *gen) create(d, coll, subject, list string) {
	g.created++
	g.commit(segment.KindCreate, d, rec{coll, g.rkey(), subject, list})
}

func listURI(owner string, n int) string { return fmt.Sprintf("at://%s/app.bsky.graph.list/l%d", owner, n) }

func (g *gen) account(d string, active bool, status string) {
	a := comatproto.SyncSubscribeRepos_Account{DID: d, Active: active, Seq: int64(g.seq + 1), Time: stamp(baseUS)}
	if status != "" {
		a.Status = gt.Some(status)
	}
	payload, err := a.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	g.push(segment.Event{Kind: segment.KindAccount, DID: d, Payload: payload},
		event{Kind: uint8(segment.KindAccount), DID: d, Active: &active, Status: status})
}

func (g *gen) identity(d string) {
	id := comatproto.SyncSubscribeRepos_Identity{DID: d, Seq: int64(g.seq + 1), Time: stamp(baseUS)}
	payload, err := id.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	g.push(segment.Event{Kind: segment.KindIdentity, DID: d, Payload: payload}, event{Kind: uint8(segment.KindIdentity), DID: d})
}

// sync archives a #sync and its replacement rows as Jetstream does: the
// repo's records then were two in three of what it held, and one new.
func (g *gen) sync(d string) {
	s := comatproto.SyncSubscribeRepos_Sync{DID: d, Rev: tid(baseUS+int64(g.seq)*1000, 5), Seq: int64(g.seq + 1), Time: stamp(baseUS), Blocks: []byte{}}
	payload, err := s.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	g.push(segment.Event{Kind: segment.KindSync, DID: d, Payload: payload}, event{Kind: uint8(segment.KindSync), DID: d})
	keys := make([]string, 0, len(g.live[d]))
	for k := range g.live[d] {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	held := g.live[d]
	g.live[d] = map[string]rec{}
	for i, k := range keys {
		if i%3 != 2 {
			g.commit(segment.KindCreateResync, d, held[k])
		}
	}
	g.commit(segment.KindCreateResync, d, rec{follow, g.rkey(), g.actors[1], ""})
}

func (g *gen) actor() string { return g.actors[g.r.below(len(g.actors))] }

func (g *gen) liveOf(d string, coll string) (rec, bool) {
	keys := make([]string, 0)
	for k, r := range g.live[d] {
		if coll == "" || r.coll == coll {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return rec{}, false
	}
	slices.Sort(keys)
	return g.live[d][keys[g.r.below(len(keys))]], true
}

func (g *gen) noise() {
	g.commit(segment.KindCreate, g.actor(), rec{like, g.rkey(), "at://" + g.actor() + "/app.bsky.feed.post/3k", ""})
}

func (g *gen) mix() {
	d := g.actor()
	switch x := g.r.below(100); {
	case x < 30:
		g.noise()
	case x < 55:
		g.create(d, follow, g.actor(), "")
	case x < 63:
		g.create(d, block, g.actor(), "")
	case x < 71:
		owner := d
		if g.r.below(7) == 0 {
			owner = g.actor()
		}
		g.create(d, listitem, g.actor(), listURI(owner, g.r.below(2)))
	case x < 83:
		if r, ok := g.liveOf(d, ""); ok {
			g.commit(segment.KindDelete, d, rec{r.coll, r.rkey, "", ""})
		} else {
			g.noise()
		}
	case x < 87:
		if r, ok := g.liveOf(d, block); ok {
			r.subject = g.actor()
			g.commit(segment.KindUpdate, d, r)
		} else {
			g.noise()
		}
	case x < 90:
		g.identity(d)
	case x < 92:
		if g.r.below(2) == 0 {
			g.account(d, false, "deactivated")
		} else {
			g.account(d, true, "")
		}
	default:
		g.create(d, follow, did(24), "")
	}
}

func main() {
	dir := ".."
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	g := &gen{r: 88172645463325252, seq: 100, live: map[string]map[string]rec{}}
	for i := range 25 {
		g.actors = append(g.actors, did(i))
	}
	n := perBlock * perSegment
	// scripted events, by the index they go in at (or after, if a sync's rows run past it)
	script := map[int]func(){
		60: func() { g.create(did(3), follow, did(4), "") },
		61: func() { g.create(did(9), block, did(2), "") },
		// a follow whose update loses its subject: the key ends empty
		70:  func() { g.create(did(10), follow, did(11), "") },
		530: func() { g.account(did(7), false, "takendown") },
		// seg 2: an account deleted and back, one deleted for good, a takedown
		// lifted, a deactivation, a sync with replacement rows, a cross-repo list item
		805: func() { g.account(did(3), false, "deleted") },
		806: func() { g.create(did(3), block, did(5), "") },
		830: func() { g.account(did(5), false, "takendown") },
		840: func() { g.account(did(6), false, "deactivated") },
		850: func() { g.account(did(9), false, "deleted") },
		860: func() {
			if r, ok := g.liveOf(did(10), follow); ok {
				r.subject = "bob.test"
				g.commit(segment.KindUpdate, did(10), r)
			}
		},
		870:  func() { g.create(did(12), listitem, did(13), listURI(did(14), 0)) },
		900:  func() { g.account(did(3), true, "") },
		950:  func() { g.sync(did(4)) },
		1100: func() { g.account(did(5), true, "") },
	}
	for i := 0; len(g.out) < n*segments; i++ {
		if f, ok := script[i]; ok {
			f()
			continue
		}
		seg, blk := len(g.out)/n, len(g.out)%n/perBlock
		if seg == 1 && blk != 2 && blk != 5 {
			g.noise()
			continue
		}
		g.mix()
	}
	g.events, g.out = g.events[:n*segments], g.out[:n*segments]

	var arch archive
	for s := range segments {
		evs := g.events[s*n : (s+1)*n]
		bb, err := segment.NewBlockBuilder(perBlock)
		if err != nil {
			panic(err)
		}
		var frames [][]byte
		var infos []blockInfo
		for i, ev := range evs {
			full, err := bb.Append(ev)
			if err != nil {
				panic(err)
			}
			if !full {
				continue
			}
			fr, _ := bb.Encode()
			frames = append(frames, fr)
			blk := evs[i+1-perBlock : i+1]
			bi := blockInfo{MinSeq: blk[0].Seq, MaxSeq: blk[len(blk)-1].Seq, Kinds: []string{}, Collections: []string{}}
			for _, e := range blk {
				k := map[segment.Kind]string{segment.KindIdentity: "identity", segment.KindAccount: "account", segment.KindSync: "sync"}[e.Kind]
				if e.Kind.IsCommit() {
					k = "commit"
					if !slices.Contains(bi.Collections, e.Collection) {
						bi.Collections = append(bi.Collections, e.Collection)
					}
				}
				if !slices.Contains(bi.Kinds, k) {
					bi.Kinds = append(bi.Kinds, k)
				}
			}
			infos = append(infos, bi)
		}
		header, footer, h, err := segment.BuildSealed(segment.SliceFrameSource(frames))
		if err != nil {
			panic(err)
		}
		file := append([]byte(nil), header...)
		for i, fr := range frames {
			infos[i].Offset = uint64(len(file))
			infos[i].Size = uint32(len(fr))
			file = binary.LittleEndian.AppendUint64(file, uint64(len(fr)))
			file = append(file, fr...)
		}
		file = append(file, footer...)
		name := fmt.Sprintf("seg_%010d.jss", s)
		if err := os.WriteFile(filepath.Join(dir, name), file, 0o644); err != nil {
			panic(err)
		}
		arch.Segments = append(arch.Segments, segInfo{
			Name: name, Checksum: fmt.Sprintf("%016x", h.Checksum), MinSeq: h.MinSeq, MaxSeq: h.MaxSeq, Blocks: infos,
		})
	}
	arch.Events = g.out
	b, err := json.Marshal(arch)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "archive.json"), append(b, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("%d events in %d segments, %d graph creates\n", len(g.out), segments, g.created)
}
