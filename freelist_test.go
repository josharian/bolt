package bolt

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"unsafe"
)

// Ensure that a page is added to a transaction's freelist.
func TestFreelist_free(t *testing.T) {
	f := newFreelist()
	f.free(100, &page{id: 12})
	if !reflect.DeepEqual([]pgid{12}, f.pending[100]) {
		t.Fatalf("exp=%v; got=%v", []pgid{12}, f.pending[100])
	}
}

// Ensure that a page and its overflow is added to a transaction's freelist.
func TestFreelist_free_overflow(t *testing.T) {
	f := newFreelist()
	f.free(100, &page{id: 12, overflow: 3})
	if exp := []pgid{12, 13, 14, 15}; !reflect.DeepEqual(exp, f.pending[100]) {
		t.Fatalf("exp=%v; got=%v", exp, f.pending[100])
	}
}

// Ensure that a transaction's free pages can be released.
func TestFreelist_release(t *testing.T) {
	f := newFreelist()
	f.free(100, &page{id: 12, overflow: 1})
	f.free(100, &page{id: 9})
	f.free(102, &page{id: 39})
	f.release(100)
	f.release(101)
	if exp := []pgid{9, 12, 13}; !reflect.DeepEqual(exp, f.ids) {
		t.Fatalf("exp=%v; got=%v", exp, f.ids)
	}

	f.release(102)
	if exp := []pgid{9, 12, 13, 39}; !reflect.DeepEqual(exp, f.ids) {
		t.Fatalf("exp=%v; got=%v", exp, f.ids)
	}
}

// Ensure that a freelist can find contiguous blocks of pages.
func TestFreelist_allocate(t *testing.T) {
	f := &freelist{ids: []pgid{3, 4, 5, 6, 7, 9, 12, 13, 18}}
	allocs := [...]struct {
		n     int
		want  pgid
		after []pgid
	}{
		{n: 3, want: 3, after: []pgid{6, 7, 9, 12, 13, 18}},
		{n: 1, want: 9, after: []pgid{6, 7, 12, 13, 18}},
		{n: 1, want: 18, after: []pgid{6, 7, 12, 13}},
		{n: 3, want: 0, after: []pgid{6, 7, 12, 13}},
		{n: 2, want: 6, after: []pgid{12, 13}},
		{n: 2, want: 12, after: []pgid{}},
		{n: 1, want: 0, after: []pgid{}},
		{n: 0, want: 0, after: []pgid{}},
	}
	for _, alloc := range allocs {
		before := make([]pgid, len(f.ids))
		copy(before, f.ids)
		got := f.allocate(alloc.n)
		if got != alloc.want {
			t.Fatalf("%v: allocate(%d) = %d want %d", before, alloc.n, got, alloc.want)
		}
		if !reflect.DeepEqual(alloc.after, f.ids) {
			t.Fatalf("%v: after allocate(%d) = %v want %v", before, alloc.n, f.ids, alloc.after)
		}
	}
}

// Ensure that a freelist can deserialize from a freelist page.
func TestFreelist_read(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = freelistPageFlag
	page.count = 2

	// Insert 2 page ids.
	ids := (*[3]pgid)(unsafe.Pointer(&page.ptr))
	ids[0] = 23
	ids[1] = 50

	// Deserialize page into a freelist.
	f := newFreelist()
	f.read(page)

	// Ensure that there are two page ids in the freelist.
	if exp := []pgid{23, 50}; !reflect.DeepEqual(exp, f.ids) {
		t.Fatalf("exp=%v; got=%v", exp, f.ids)
	}
}

// Ensure that a freelist can serialize into a freelist page.
func TestFreelist_write(t *testing.T) {
	// Create a freelist and write it to a page.
	var buf [4096]byte
	f := &freelist{ids: []pgid{12, 39}, pending: make(map[txid][]pgid)}
	f.pending[100] = []pgid{28, 11}
	f.pending[101] = []pgid{3}
	p := (*page)(unsafe.Pointer(&buf[0]))
	if err := f.write(p); err != nil {
		t.Fatal(err)
	}

	// Read the page back out.
	f2 := newFreelist()
	f2.read(p)

	// Ensure that the freelist is correct.
	// All pages should be present and in reverse order.
	if exp := []pgid{3, 11, 12, 28, 39}; !reflect.DeepEqual(exp, f2.ids) {
		t.Fatalf("exp=%v; got=%v", exp, f2.ids)
	}
}

func Benchmark_FreelistRelease10K(b *testing.B)    { benchmark_FreelistRelease(b, 10000) }
func Benchmark_FreelistRelease100K(b *testing.B)   { benchmark_FreelistRelease(b, 100000) }
func Benchmark_FreelistRelease1000K(b *testing.B)  { benchmark_FreelistRelease(b, 1000000) }
func Benchmark_FreelistRelease10000K(b *testing.B) { benchmark_FreelistRelease(b, 10000000) }

func benchmark_FreelistRelease(b *testing.B, size int) {
	ids := randomPgids(size)
	pending := randomPgids(len(ids) / 400)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := &freelist{ids: ids, pending: map[txid][]pgid{1: pending}}
		f.release(1)
	}
}

func randomPgids(n int) []pgid {
	rand.Seed(42)
	pgids := make(pgids, n)
	for i := range pgids {
		pgids[i] = pgid(rand.Int63())
	}
	sort.Sort(pgids)
	return pgids
}
