package vclock_test

import (
	"bytes"
	"encoding/json"
	"github.com/jbondeson/vclock"
	. "launchpad.net/gocheck"
	"testing"
)

func TestAll(c *testing.T) {
	TestingT(c)
}

type S struct{}

var testSuite = Suite(&S{})

func (S) TestLastUpdate(c *C) {
	vc := vclock.New()
	c.Assert(vc.LastUpdate(), Equals, uint64(0))
	vc.Update("idA", 5)
	c.Assert(vc.LastUpdate(), Equals, uint64(5))
	vc.Update("idB", 3)
	c.Assert(vc.LastUpdate(), Equals, uint64(5))
	vc.Update("idC", 7)
	c.Assert(vc.LastUpdate(), Equals, uint64(7))
	vc.Update("idB", 9)
	c.Assert(vc.LastUpdate(), Equals, uint64(9))
	vc.Update("idB", 7)
	c.Assert(vc.LastUpdate(), Equals, uint64(9))
}

func (S) TestUpdateAndCompare(c *C) {
	vc1 := vclock.New()
	vc2 := vclock.New()
	c.Assert(vc1.Compare(vc2, vclock.Equal), Equals, true)
	c.Assert(vc1.Compare(vc2, ^vclock.Equal), Equals, false)
	vc2.Update("idA", 0)
	c.Assert(vc1.Compare(vc2, vclock.Descendant), Equals, true)
	c.Assert(vc1.Compare(vc2, vclock.Equal), Equals, false)
	c.Assert(vc1.Compare(vc2, ^vclock.Descendant), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Ancestor), Equals, true)
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, false)
	c.Assert(vc2.Compare(vc1, ^vclock.Ancestor), Equals, false)
	vc1.Update("idA", 0)
	c.Assert(vc1.Compare(vc2, vclock.Equal), Equals, true)
	c.Assert(vc1.Compare(vc2, ^vclock.Equal), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, true)
	c.Assert(vc2.Compare(vc1, ^vclock.Equal), Equals, false)
	vc1.Update("idB", 0)
	c.Assert(vc1.Compare(vc2, vclock.Ancestor), Equals, true)
	c.Assert(vc1.Compare(vc2, ^vclock.Ancestor), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Descendant), Equals, true)
	c.Assert(vc2.Compare(vc1, ^vclock.Descendant), Equals, false)
	vc2.Update("idA", 0)
	c.Assert(vc1.Compare(vc2, vclock.Concurrent), Equals, true)
	c.Assert(vc1.Compare(vc2, ^vclock.Concurrent), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Concurrent), Equals, true)
	c.Assert(vc2.Compare(vc1, ^vclock.Concurrent), Equals, false)
	vc2.Update("idB", 0)
	c.Assert(vc1.Compare(vc2, vclock.Descendant), Equals, true)
	c.Assert(vc1.Compare(vc2, ^vclock.Descendant), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Ancestor), Equals, true)
	c.Assert(vc2.Compare(vc1, ^vclock.Ancestor), Equals, false)
	vc2.Update("idB", 0)
	c.Assert(vc1.Compare(vc2, vclock.Descendant), Equals, true)
	c.Assert(vc1.Compare(vc2, ^vclock.Descendant), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Ancestor), Equals, true)
	c.Assert(vc2.Compare(vc1, ^vclock.Ancestor), Equals, false)
}

func (S) TestCompareWithMissingInOther(c *C) {
	vc1 := vclock.New()
	vc1.Update("idA", 1)
	vc2 := vc1.Copy()
	vc2.Update("idB", 1)
	vc1.Update("idC", 5)
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Ancestor), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Descendant), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Concurrent), Equals, true)
	vc1.Update("idD", 5)
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Ancestor), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Descendant), Equals, false)
	c.Assert(vc2.Compare(vc1, vclock.Concurrent), Equals, true)
}

func (S) TestMerge(c *C) {
	vc1 := vclock.New()
	vc2 := vclock.New()
	vc1.Update("idA", 0)
	vc1.Update("idA", 0) // counter > than vc2
	vc1.Update("idB", 0) // counter < than vc2
	vc1.Update("idC", 0) // counter not in vc2
	vc2.Update("idA", 0)
	vc2.Update("idB", 0)
	vc2.Update("idB", 0)
	vc2.Update("idD", 0) // counter not in vc1

	vc2.Merge(vc1)

	vc1.Update("idB", 0)
	vc1.Update("idD", 0)
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, true)
}

func (S) TestCopy(c *C) {
	vc1 := vclock.New()
	vc1.Update("idA", 0)
	vc2 := vc1.Copy()
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, true)
	vc2.Update("idA", 0)
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, false)
	vc1.Update("idA", 0)
	vc2.Update("idB", 0)
	c.Assert(vc2.Compare(vc1, vclock.Equal), Equals, false)
}

func (S) TestBytesSimple(c *C) {
	// The basic format of the bytes representation is:
	// [ header byte | [ ticks | id len | id ] * N ]
	vc := vclock.New()
	c.Assert(len(vc.Bytes()), Equals, 0)
	testFromBytes(c, vc)
	vc.Update("idA", 0)
	c.Assert(vc.Bytes(), DeepEquals, []byte{0, 1, 3, 'i', 'd', 'A'})
	testFromBytes(c, vc)
	vc.Update("idB", 0)
	vc.Update("idB", 0)
	c.Assert(vc.Bytes(), DeepEquals, []byte{0, 1, 3, 'i', 'd', 'A', 2, 3, 'i', 'd', 'B'})
	testFromBytes(c, vc)
}

func (S) TestBytesTicksPacking(c *C) {
	vc := vclock.New()
	for i := 0; i != 127; i++ {
		vc.Update("idA", 0)
	}

	// Ticks = 127
	// 127 = 0 1111111 - Continuation bit off.
	c.Assert(vc.Bytes(), DeepEquals, []byte{0, 127, 3, 'i', 'd', 'A'})
	testFromBytes(c, vc)

	vc.Update("idA", 0)

	// Ticks = 128
	// 129 = 1 0000001 - Continuation bit on + 8th bit.
	//   0 = 0 0000000 - Lower 7 bits.
	c.Assert(vc.Bytes(), DeepEquals, []byte{0, 129, 0, 3, 'i', 'd', 'A'})
	testFromBytes(c, vc)

	for i := 0; i != 127; i++ {
		vc.Update("idA", 0)
	}

	// Ticks = 255
	// 129 = 1 0000001 - Continuation bit on + 8th bit.
	// 127 = 0 1111111 - Lower 7 bits.
	c.Assert(vc.Bytes(), DeepEquals, []byte{0, 129, 127, 3, 'i', 'd', 'A'})
	testFromBytes(c, vc)

	vc.Update("idA", 0)

	// Ticks = 256
	// 129 = 1 0000010 - Continuation bit on + 9th bit.
	//   0 = 0 0000000 - Lower 7 bits.
	c.Assert(vc.Bytes(), DeepEquals, []byte{0, 130, 0, 3, 'i', 'd', 'A'})
	testFromBytes(c, vc)
}

func (S) TestBytesIdLenPacking(c *C) {
	vc := vclock.New()

	id := make([]byte, 255)
	for i := 0; i != len(id); i++ {
		id[i] = 'X'
	}

	vc.Update(string(id), 0)

	result := vc.Bytes()

	// len(id) = 255
	// 129 = 1 0000001 - Continuation bit on + 8th bit.
	// 127 = 0 1111111 - Lower 7 bits.
	c.Assert(result[:4], DeepEquals, []byte{0, 1, 129, 127})
	c.Assert(result[4:], DeepEquals, id)
	testFromBytes(c, vc)
}

func (S) TestBytesWithTime(c *C) {
	// When there's one or more values with time, the format becomes:
	// [ header byte | [ ticks | time | id len | id ] * N ]
	vc := vclock.New()
	vc.Update("idA", 15)
	c.Assert(vc.Bytes(), DeepEquals, []byte{1, 1, 15, 3, 'i', 'd', 'A'})
	testFromBytes(c, vc)
	vc.Update("idB", 5)
	vc.Update("idB", 255)
	c.Assert(vc.Bytes(), DeepEquals, []byte{1, 1, 15, 3, 'i', 'd', 'A', 2, 129, 127, 3, 'i', 'd', 'B'})
	testFromBytes(c, vc)
}

type fromBytesTest struct {
	header byte
	suffix []byte
	error  string
}

var fromBytesBadData = []fromBytesTest{
	// Unknown bits in header.
	fromBytesTest{128, []byte{}, "bad vclock header"},
	// Missing id information after ticks is unpacked.
	fromBytesTest{0, []byte{1}, "bad vclock ticks"},
	// Improperly packed ticks (missing termination byte).
	fromBytesTest{0, []byte{129}, "bad vclock ticks"},
	// Missing id of length 1 after correctly packed id length.
	fromBytesTest{0, []byte{2, 1}, "bad vclock id"},
	// Improperly packed id length (missing termination byte).
	fromBytesTest{0, []byte{2, 129}, "bad vclock id"},
	// Bad id length (says 2, but got only 1 byte).
	fromBytesTest{0, []byte{2, 2, 'X'}, "bad vclock id"},
	// Missing id information after ticks and time is unpacked.
	fromBytesTest{1, []byte{3, 2}, "bad vclock id"},
	// Improperly packed time (missing termination byte).
	fromBytesTest{1, []byte{3, 129}, "bad vclock time"},
}

func (S) TestFromBytesWithBadData(c *C) {
	for i, test := range fromBytesBadData {
		var prefix []byte
		if (test.header & 1) != 0 {
			prefix = []byte{test.header, 1, 2, 3, 'i', 'd', 'A'} // With time.
		} else {
			prefix = []byte{test.header, 1, 3, 'i', 'd', 'A'} // Without time.
		}
		value := bytes.Join([][]byte{prefix, test.suffix}, []byte{})
		vc, err := vclock.FromBytes(value)
		c.Assert(err, ErrorMatches, test.error, Commentf("%#d failed", i))
		c.Assert(vc, IsNil)
	}
}

func testFromBytes(c *C, vc1 *vclock.VClock) {
	vc1Bytes := vc1.Bytes()
	vc2, err := vclock.FromBytes(vc1Bytes)
	c.Assert(err, Equals, nil)
	c.Assert(vc2.Bytes(), DeepEquals, vc1Bytes)
}

type truncItem struct {
	id      string
	updates int
	time    uint64
}

type truncTest struct {
	summary string
	trunc   vclock.Truncation
	before  []truncItem
	after   []truncItem
}

var truncTests = []truncTest{
	truncTest{
		"Do nothing",
		vclock.Truncation{},
		[]truncItem{},
		[]truncItem{},
	},
	truncTest{
		"Do nothing again",
		vclock.Truncation{},
		[]truncItem{truncItem{"idA", 1, 2}},
		[]truncItem{truncItem{"idA", 1, 2}},
	},
	truncTest{
		"Do nothing with 'CutBefore'",
		vclock.Truncation{CutBefore: 2},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
	},
	truncTest{
		"Cut out with 'CutBefore'",
		vclock.Truncation{CutBefore: 3},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
		[]truncItem{truncItem{"idB", 1, 3}},
	},
	truncTest{
		"Preserve with 'CutBefore' and 'KeepMinN'",
		vclock.Truncation{CutBefore: 3, KeepMinN: 2},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
	},
	truncTest{
		"Cut out with 'CutBefore' and 'KeepMinN'",
		vclock.Truncation{CutBefore: 3, KeepMinN: 1},
		// Note that order matters here. A bad implementation might look at
		// lastUpdate=2 and note that min wasn'c reached yet, and include it.
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
		[]truncItem{truncItem{"idB", 1, 3}},
	},
	truncTest{
		"Do nothing with 'CutAboveN'",
		vclock.Truncation{CutAboveN: 2},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
	},
	truncTest{
		"Cut out with 'CutAboveN'",
		vclock.Truncation{CutAboveN: 1},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
		// Note that B was preserved, not A, due to the update time.
		[]truncItem{truncItem{"idB", 1, 3}},
	},
	truncTest{
		"Preserve with 'CutAboveN' and 'KeepAfter'",
		vclock.Truncation{CutAboveN: 1, KeepAfter: 1},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
		[]truncItem{truncItem{"idA", 1, 2}, truncItem{"idB", 1, 3}},
	},
}

func (s *S) TestTruncate(c *C) {
	for testN, test := range truncTests {
		before := vclock.New()
		after := vclock.New()
		for i := 0; i != len(test.before); i++ {
			for j := 0; j != test.before[i].updates; j++ {
				before.Update(test.before[i].id, test.before[i].time)
			}
		}
		for i := 0; i != len(test.after); i++ {
			for j := 0; j != test.after[i].updates; j++ {
				after.Update(test.after[i].id, test.after[i].time)
			}
		}
		truncated := before.Truncate(&test.trunc)
		cmt := Commentf("Truncation test %d failed: %s: %#v", testN, test.summary, truncated.Bytes())
		c.Assert(truncated.Compare(after, vclock.Equal), Equals, true, cmt)
	}
}

func (s *S) TestJson(c *C) {
	vc1 := vclock.New()
	vc1.Update("a", 1)

	j1, _ := json.Marshal(vc1)

	var vc2 *vclock.VClock
	_ = json.Unmarshal(j1, &vc2)

	c.Assert(vc1, DeepEquals, vc2)
}
