package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	spans   []freespan          // all free and available free page spans.
	pending map[txid][]freespan // mapping of soon-to-be free page spans by tx; each is sorted.
}

// newFreelist returns an empty, initialized freelist.
func newFreelist() *freelist {
	return &freelist{pending: make(map[txid][]freespan)}
}

// pagesize returns the size of the freelist page after serialization.
func (f *freelist) pagesize() int {
	n := f.spancount()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(freespanZero)) * n)
}

// spancount returns the number of spans in the freelist. It may overcount.
func (f *freelist) spancount() int {
	// This is a floor. Some of the free and pending spans may be mergeable.
	return f.freeSpanCount() + f.pendingSpanCount()
}

// freeSpanCount returns the number of free spans.
func (f *freelist) freeSpanCount() int {
	return len(f.spans)
}

// pendingSpanCount returns the number of pending spans. It may overcount.
func (f *freelist) pendingSpanCount() int {
	var n int
	// This is a floor. Some of these pending spans may be mergeable.
	for _, list := range f.pending {
		n += len(list)
	}
	return n
}

// pagecount returns the number of pages on the freelist.
func (f *freelist) pagecount() int {
	return f.freePageCount() + f.pendingPageCount()
}

// freePageCount returns the number of free pages on the freelist.
func (f *freelist) freePageCount() int {
	var n int
	for _, span := range f.spans {
		n += int(span.size())
	}
	return n
}

// pendingPageCount returns the number of pending pages on the freelist.
func (f *freelist) pendingPageCount() int {
	var n int
	for _, list := range f.pending {
		for _, span := range list {
			n += int(span.size())
		}
	}
	return n
}

// allpages returns an unsorted list of all free ids and all pending ids.
// It should only be called from performance-insensitive code.
func (f *freelist) allpages() []pgid {
	ids := make(pgids, 0, f.pagecount())
	for _, span := range f.spans {
		ids = span.appendAll(ids)
	}
	for _, list := range f.pending {
		for _, span := range list {
			ids = span.appendAll(ids)
		}
	}
	return ids
}

// copyall copies into dst a normalized, sorted list combining all free and pending spans.
// It returns the number of spans copied into dst.
// f.spancount returns a safe minimum length for dst.
func (f *freelist) copyall(dst []freespan) int {
	all := make([][]freespan, 0, len(f.pending)+1)
	all = append(all, f.spans)
	for _, list := range f.pending {
		all = append(all, list)
	}
	return len(mergenorm(dst, all))
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
func (f *freelist) allocate(n int) pgid {
	for i, span := range f.spans {
		if span.start() <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", span.start()))
		}
		if span.size() < uint64(n) {
			continue
		}
		// TODO: search for a better-sized match.
		// Use the first n elements of this span.
		// This might result in a span of size 0.
		// That is ok; it will be cleaned up when merging freespans.
		f.spans[i] = makeFreespan(span.start()+pgid(n), span.size()-uint64(n))
		return span.start()
	}
	return 0
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}
	// Free p and all its overflow pages.
	pspan := makeFreespan(p.id, uint64(p.overflow)+1)
	var spans = f.pending[txid]
	n := sort.Search(len(spans), func(i int) bool { return spans[i] > pspan })
	if n == len(spans) {
		spans = append(spans, pspan)
	} else {
		u, v := pspan.append(spans[n])
		if v == 0 {
			// spans[n] and pspan were combined. Replace spans[n] with the new value.
			spans[n] = u
		} else {
			// Insert new span.
			spans = append(spans, 0)
			copy(spans[n+1:], spans[n:])
			spans[n] = u
			spans[n+1] = v
		}
	}
	f.pending[txid] = spans
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	all := make([][]freespan, 0, len(f.pending)+1)
	all = append(all, f.spans)
	for tid, spans := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			all = append(all, spans)
			delete(f.pending, tid)
		}
	}
	f.spans = mergenorm(nil, all)
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {
	// Remove pages from pending list.
	delete(f.pending, txid)
}

// freed reports whether a given page is in the free list.
func (f *freelist) freed(pgid pgid) bool {
	if freespans(f.spans).contains(pgid) {
		return true
	}
	for _, s := range f.pending {
		if freespans(s).contains(pgid) {
			return true
		}
	}
	return false
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.spans = nil
	} else {
		spans := ((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.spans = make([]freespan, len(spans))
		copy(f.spans, spans)

		// Make sure they're sorted.
		// TODO: eliminate? By construction, they are sorted.
		// Or instead, panic if not sorted?
		sort.Slice(f.spans, func(i, j int) bool { return f.spans[i] < f.spans[j] })
	}

	// Rebuild the page cache.
	// TODO: normalize or something?
	// f.reindex()
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
func (f *freelist) write(p *page) {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements.
	// If we might overflow that number then we put the size in the first element.
	n := f.spancount()
	switch {
	case n == 0:
		p.count = 0
	case n < 0xFFFF:
		n = f.copyall(((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[:])
		p.count = uint16(n)
	default:
		p.count = 0xFFFF
		n = f.copyall(((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[1:])
		((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[0] = freespan(n)
	}
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *page) {
	f.read(p)

	// TODO: optimize this some?

	// Gather all pending spans into a single list.
	all := make([][]freespan, 0, len(f.pending))
	for _, spans := range f.pending {
		all = append(all, spans)
	}
	pending := mergenorm(nil, all)

	// Remove all pending spans from f.spans.
	for _, rm := range pending {
		n := sort.Search(len(f.spans), func(i int) bool { return f.spans[i] > rm })
		// n is where rm would be inserted.
		// Every element to remove must be a sub-span of some span in f.spans,
		// so n cannot have a start greater than the largest start in f.spans,
		// nor have it have an equal start or greater size.
		// Therefore, n != len(f.spans).

		// If rm is a strict prefix of one of f's spans,
		// the containing span will be at n.
		// Otherwise, it'll be at n-1.
		if s := f.spans[n]; rm.start() == s.start() {
			f.spans[n] = makeFreespan(s.start()+pgid(rm.size()), uint64(s.size())-rm.size())
			continue
		}

		s := f.spans[n-1]
		if s.start() == rm.start() {
			// Exact match.
			if rm.size() != s.size() {
				panic("sort.Search misuse?")
			}
			f.spans[n-1] = makeFreespan(s.start(), 0)
			continue
		}

		if !s.contains(rm.start()) {
			panic("sort.Search misuse (part b)?")
		}

		if s.next() == rm.next() {
			// rm is a suffix of s.
			f.spans[n-1] = makeFreespan(s.start(), s.size()-rm.size())
			continue
		}

		// rm splits s into two parts.
		// TODO: this insertion business could lead to quadratic behavior!
		f.spans = append(f.spans, 0)
		copy(f.spans[n:], f.spans[n-1:])
		f.spans[n-1] = makeFreespan(s.start(), uint64(rm.start()-s.start()))
		f.spans[n] = makeFreespan(rm.next(), uint64(s.next()-rm.next()))
	}
}
