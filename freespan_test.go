package bolt

import "testing"

func TestFreespanBasics(t *testing.T) {
	// Basic sanity checks.
	tests := [...]struct {
		start pgid
		size  uint64
	}{
		{start: 0, size: 0},
		{start: 7, size: 0},
		{start: 15, size: 7},
		{start: 3, size: freespanMaxSize},
		{start: freespanMaxStart, size: 12},
	}
	for _, test := range tests {
		s := makeFreespan(test.start, test.size)
		if s.start() != test.start || s.size() != test.size {
			t.Errorf("%v did not round trip, got %v", test, s)
			continue
		}
		if test.size == freespanMaxSize {
			continue
		}
		for i := s.start(); i < s.next(); i++ {
			if !s.contains(i) {
				t.Errorf("%v should contain %d", s, i)
			}
		}
		if i := s.start() - 1; s.contains(i) {
			t.Error("%v should not contain %d", s, i)
		}
		if i := s.next(); s.contains(i) {
			t.Error("%v should not contain %d", s, i)
		}

	}
}

func TestFreespanAppend(t *testing.T) {
	tests := [...]struct {
		s, t freespan // inputs
		u, v freespan // expected outputs
	}{
		{s: 0, t: 0, u: 0, v: 0},
		{s: makeFreespan(12, 13), t: makeFreespan(35, 0), u: makeFreespan(12, 13), v: 0},
		{s: makeFreespan(10, 0), t: makeFreespan(12, 13), u: makeFreespan(12, 13), v: 0},
		{s: makeFreespan(10, 1), t: makeFreespan(11, 3), u: makeFreespan(10, 4), v: 0},
		{s: makeFreespan(10, 0), t: makeFreespan(10, 3), u: makeFreespan(10, 3), v: 0},
		{s: makeFreespan(10, 1), t: makeFreespan(12, 3), u: makeFreespan(10, 1), v: makeFreespan(12, 3)},
		{
			s: makeFreespan(10, freespanMaxSize),
			t: makeFreespan(10+freespanMaxSize, 3),
			u: makeFreespan(10, freespanMaxSize),
			v: makeFreespan(10+freespanMaxSize, 3),
		},
		{
			s: makeFreespan(10, freespanMaxSize-2),
			t: makeFreespan(10+freespanMaxSize-2, 3),
			u: makeFreespan(10, freespanMaxSize),
			v: makeFreespan(10+freespanMaxSize, 1),
		},
	}
	for _, test := range tests {
		u, v := test.s.append(test.t)
		if u != test.u || v != test.v {
			t.Errorf("%v.append(%v) = %v, %v want %v, %v", test.s, test.t, u, v, test.u, test.v)
		}
	}
}
