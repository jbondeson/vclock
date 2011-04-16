package govclock_test


import (
    . "launchpad.net/gocheck"
    "testing"
    "govclock"
    "bytes"
)


var idA = []byte("idA")
var idB = []byte("idB")
var idC = []byte("idC")
var idD = []byte("idD")


func TestAll(c *testing.T) {
    TestingT(c)
}


type S struct{}

var testSuite = Suite(&S{})


func (s *S) TestLastUpdate(c *C) {
    vc := govclock.New()
    c.Assert(vc.LastUpdate(), Equals, uint64(0))
    vc.Update(idA, 5)
    c.Assert(vc.LastUpdate(), Equals, uint64(5))
    vc.Update(idB, 3)
    c.Assert(vc.LastUpdate(), Equals, uint64(5))
    vc.Update(idC, 7)
    c.Assert(vc.LastUpdate(), Equals, uint64(7))
    vc.Update(idB, 9)
    c.Assert(vc.LastUpdate(), Equals, uint64(9))
    vc.Update(idB, 7)
    c.Assert(vc.LastUpdate(), Equals, uint64(9))
}

func (s *S) TestUpdateAndCompare(c *C) {
    vc1 := govclock.New()
    vc2 := govclock.New()
    c.Assert(vc1.Compare(vc2, govclock.Equal), Equals, true)
    c.Assert(vc1.Compare(vc2, ^govclock.Equal), Equals, false)
    vc2.Update(idA, 0)
    c.Assert(vc1.Compare(vc2, govclock.Descendant), Equals, true)
    c.Assert(vc1.Compare(vc2, govclock.Equal), Equals, false)
    c.Assert(vc1.Compare(vc2, ^govclock.Descendant), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Ancestor), Equals, true)
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, false)
    c.Assert(vc2.Compare(vc1, ^govclock.Ancestor), Equals, false)
    vc1.Update(idA, 0)
    c.Assert(vc1.Compare(vc2, govclock.Equal), Equals, true)
    c.Assert(vc1.Compare(vc2, ^govclock.Equal), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, true)
    c.Assert(vc2.Compare(vc1, ^govclock.Equal), Equals, false)
    vc1.Update(idB, 0)
    c.Assert(vc1.Compare(vc2, govclock.Ancestor), Equals, true)
    c.Assert(vc1.Compare(vc2, ^govclock.Ancestor), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Descendant), Equals, true)
    c.Assert(vc2.Compare(vc1, ^govclock.Descendant), Equals, false)
    vc2.Update(idA, 0)
    c.Assert(vc1.Compare(vc2, govclock.Concurrent), Equals, true)
    c.Assert(vc1.Compare(vc2, ^govclock.Concurrent), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Concurrent), Equals, true)
    c.Assert(vc2.Compare(vc1, ^govclock.Concurrent), Equals, false)
    vc2.Update(idB, 0)
    c.Assert(vc1.Compare(vc2, govclock.Descendant), Equals, true)
    c.Assert(vc1.Compare(vc2, ^govclock.Descendant), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Ancestor), Equals, true)
    c.Assert(vc2.Compare(vc1, ^govclock.Ancestor), Equals, false)
    vc2.Update(idB, 0)
    c.Assert(vc1.Compare(vc2, govclock.Descendant), Equals, true)
    c.Assert(vc1.Compare(vc2, ^govclock.Descendant), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Ancestor), Equals, true)
    c.Assert(vc2.Compare(vc1, ^govclock.Ancestor), Equals, false)
}

func (s *S) TestCompareWithMissingInOther(c *C) {
    vc1 := govclock.New()
    vc1.Update(idA, 1)
    vc2 := vc1.Copy()
    vc2.Update(idB, 1)
    vc1.Update(idC, 5)
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Ancestor), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Descendant), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Concurrent), Equals, true)
    vc1.Update(idD, 5)
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Ancestor), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Descendant), Equals, false)
    c.Assert(vc2.Compare(vc1, govclock.Concurrent), Equals, true)
}

func (s *S) TestUpdateCopiesBytes(c *C) {
    var id [1]byte
    id[0] = 'A'
    vc := govclock.New()
    vc.Update(id[:], 0)
    id[0] = 'B'
    c.Assert(vc.Bytes(), Equals, []byte{0,1,1,'A'})
}

func (s *S) TestMerge(c *C) {
    vc1 := govclock.New()
    vc2 := govclock.New()
    vc1.Update(idA, 0)
    vc1.Update(idA, 0) // counter > than vc2
    vc1.Update(idB, 0) // counter < than vc2
    vc1.Update(idC, 0) // counter not in vc2
    vc2.Update(idA, 0)
    vc2.Update(idB, 0)
    vc2.Update(idB, 0)
    vc2.Update(idD, 0) // counter not in vc1

    vc2.Merge(vc1)

    vc1.Update(idB, 0)
    vc1.Update(idD, 0)
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, true)
}

func (s *S) TestCopy(c *C) {
    vc1 := govclock.New()
    vc1.Update(idA, 0)
    vc2 := vc1.Copy()
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, true)
    vc2.Update(idA, 0)
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, false)
    vc1.Update(idA, 0)
    vc2.Update(idB, 0)
    c.Assert(vc2.Compare(vc1, govclock.Equal), Equals, false)
}

func (s *S) TestBytesSimple(c *C) {
    // The basic format of the bytes representation is:
    // [ header byte | [ ticks | id len | id ] * N ]
    vc := govclock.New()
    c.Assert(len(vc.Bytes()), Equals, 0)
    testFromBytes(c, vc)
    vc.Update(idA, 0)
    c.Assert(vc.Bytes(), Equals, []byte{0,1,3,'i','d','A'})
    testFromBytes(c, vc)
    vc.Update(idB, 0)
    vc.Update(idB, 0)
    c.Assert(vc.Bytes(), Equals, []byte{0,1,3,'i','d','A',2,3,'i','d','B'})
    testFromBytes(c, vc)
}

func (s *S) TestBytesTicksPacking(c *C) {
    vc := govclock.New()
    for i := 0; i != 127; i++ {
        vc.Update(idA, 0)
    }

    // Ticks = 127
    // 127 = 0 1111111 - Continuation bit off.
    c.Assert(vc.Bytes(), Equals, []byte{0,127,3,'i','d','A'})
    testFromBytes(c, vc)

    vc.Update(idA, 0)

    // Ticks = 128
    // 129 = 1 0000001 - Continuation bit on + 8th bit.
    //   0 = 0 0000000 - Lower 7 bits.
    c.Assert(vc.Bytes(), Equals, []byte{0,129,0,3,'i','d','A'})
    testFromBytes(c, vc)

    for i := 0; i != 127; i++ {
        vc.Update(idA, 0)
    }

    // Ticks = 255
    // 129 = 1 0000001 - Continuation bit on + 8th bit.
    // 127 = 0 1111111 - Lower 7 bits.
    c.Assert(vc.Bytes(), Equals, []byte{0,129,127,3,'i','d','A'})
    testFromBytes(c, vc)

    vc.Update(idA, 0)

    // Ticks = 256
    // 129 = 1 0000010 - Continuation bit on + 9th bit.
    //   0 = 0 0000000 - Lower 7 bits.
    c.Assert(vc.Bytes(), Equals, []byte{0,130,0,3,'i','d','A'})
    testFromBytes(c, vc)
}

func (s *S) TestBytesIdLenPacking(c *C) {
    vc := govclock.New()

    id := make([]byte, 255)
    for i := 0; i != len(id); i++ {
        id[i] = 'X'
    }

    vc.Update(id, 0)

    result := vc.Bytes()

    // len(id) = 255
    // 129 = 1 0000001 - Continuation bit on + 8th bit.
    // 127 = 0 1111111 - Lower 7 bits.
    c.Assert(result[:4], Equals, []byte{0,1,129,127})
    c.Assert(result[4:], Equals, id)
    testFromBytes(c, vc)
}

func (s *S) TestBytesWithTime(c *C) {
    // When there's one or more values with time, the format becomes:
    // [ header byte | [ ticks | time | id len | id ] * N ]
    vc := govclock.New()
    vc.Update(idA, 15)
    c.Assert(vc.Bytes(), Equals, []byte{1,1,15,3,'i','d','A'})
    testFromBytes(c, vc)
    vc.Update(idB, 5)
    vc.Update(idB, 255)
    c.Assert(vc.Bytes(), Equals,
             []byte{1,1,15,3,'i','d','A',2,129,127,3,'i','d','B'})
    testFromBytes(c, vc)
}


type fromBytesTest struct {
    header byte
    suffix []byte
    error string
}


var fromBytesBadData = []fromBytesTest{
    // Unknown bits in header.
    fromBytesTest{128, []byte{},        "Bad vclock header"},
    // Missing id information after ticks is unpacked.
    fromBytesTest{0,   []byte{1},       "Bad vclock data (ticks)"},
    // Improperly packed ticks (missing termination byte).
    fromBytesTest{0,   []byte{129},     "Bad vclock data (ticks)"},
    // Missing id of length 1 after correctly packed id length.
    fromBytesTest{0,   []byte{2,1},     "Bad vclock data (id)"},
    // Improperly packed id length (missing termination byte).
    fromBytesTest{0,   []byte{2,129},   "Bad vclock data (id)"},
    // Bad id length (says 2, but got only 1 byte).
    fromBytesTest{0,   []byte{2,2,'X'}, "Bad vclock data (id)"},
    // Missing id information after ticks and time is unpacked.
    fromBytesTest{1,   []byte{3,2},     "Bad vclock data (id)"},
    // Improperly packed time (missing termination byte).
    fromBytesTest{1,   []byte{3,129},   "Bad vclock data (time)"},
}


func (s *S) TestFromBytesWithBadData(c *C) {
    for i, test := range fromBytesBadData {
        var prefix []byte
        if (test.header & 1) != 0 {
            prefix = []byte{test.header,1,2,3,'i','d','A'} // With time.
        } else {
            prefix = []byte{test.header,1,3,'i','d','A'} // Without time.
        }
        value := bytes.Join([][]byte{prefix, test.suffix}, []byte{})
        vc, err := govclock.FromBytes(value)
        c.Assert(err, NotNil)
        c.Assert(err.String(), Equals, test.error,
                 Bug("#%d failed", i))
        if err.String() == "Bad vclock header" {
            c.Assert(vc.Bytes(), Equals, []byte{},
                     Bug("#%d failed", i))
        } else {
            c.Assert(vc.Bytes(), Equals, prefix,
                     Bug("#%d failed", i))
        }
    }
}

func testFromBytes(c *C, vc1 *govclock.VClock) {
    vc1Bytes := vc1.Bytes()
    vc2, err := govclock.FromBytes(vc1Bytes)
    c.Assert(err, Equals, nil)
    c.Assert(vc2.Bytes(), Equals, vc1Bytes)
}


type truncItem struct {
    id []byte
    updates int
    time uint64
}

type truncTest struct {
    summary string
    spec govclock.TruncateSpec
    before []truncItem
    after []truncItem
}

var truncTests = []truncTest{
    truncTest{
        "Do nothing",
        govclock.TruncateSpec{},
        []truncItem{},
        []truncItem{},
    },
    truncTest{
        "Do nothing again",
        govclock.TruncateSpec{},
        []truncItem{truncItem{idA, 1, 2}},
        []truncItem{truncItem{idA, 1, 2}},
    },
    truncTest{
        "Do nothing with 'CutBefore'",
        govclock.TruncateSpec{CutBefore: 2},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
    },
    truncTest{
        "Cut out with 'CutBefore'",
        govclock.TruncateSpec{CutBefore: 3},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
        []truncItem{truncItem{idB, 1, 3}},
    },
    truncTest{
        "Preserve with 'CutBefore' and 'KeepMinN'",
        govclock.TruncateSpec{CutBefore: 3, KeepMinN: 2},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
    },
    truncTest{
        "Cut out with 'CutBefore' and 'KeepMinN'",
        govclock.TruncateSpec{CutBefore: 3, KeepMinN: 1},
        // Note that order matters here. A bad implementation might look at
        // lastUpdate=2 and note that min wasn'c reached yet, and include it.
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
        []truncItem{truncItem{idB, 1, 3}},
    },
    truncTest{
        "Do nothing with 'CutAboveN'",
        govclock.TruncateSpec{CutAboveN: 2},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
    },
    truncTest{
        "Cut out with 'CutAboveN'",
        govclock.TruncateSpec{CutAboveN: 1},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
        // Note that B was preserved, not A, due to the update time.
        []truncItem{truncItem{idB, 1, 3}},
    },
    truncTest{
        "Preserve with 'CutAboveN' and 'KeepAfter'",
        govclock.TruncateSpec{CutAboveN: 1, KeepAfter: 1},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
        []truncItem{truncItem{idA, 1, 2}, truncItem{idB, 1, 3}},
    },
}

func (s *S) TestTruncate(c *C) {
    for testN, test := range truncTests {
        before := govclock.New()
        after := govclock.New()
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
        truncated := before.Truncate(test.spec)
        c.Assert(truncated.Compare(after, govclock.Equal), Equals, true,
                 Bug("Truncation test %d failed: %s: %#v",
                     testN, test.summary, truncated.Bytes()))
    }
}
