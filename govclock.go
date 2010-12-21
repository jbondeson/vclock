package govclock

import (
    "bytes"
    "math"
    "sort"
    "os"
)


const (
    Equal      = 1 << iota
    Ancestor
    Descendant
    Concurrent
)


type itemType struct {
    id []byte
    ticks uint64
    lastUpdate uint64
}

type VClock struct {
    hasUpdateTime bool
    items []itemType
}

// Find the index for the item with the given id.
func (vc *VClock) findItem(id []byte) (index int, found bool) {
    for i := 0; i != len(vc.items); i++ {
        if bytes.Equal(vc.items[i].id, id) {
            return i, true
        }
    }
    return 0, false
}

// Change or append the given id.
func (vc *VClock) updateItem(id []byte, ticks, when uint64) {
    if when > 0 {
        vc.hasUpdateTime = true
    }
    if i, found := vc.findItem(id); found {
        vc.items[i].ticks += ticks
        if when > vc.items[i].lastUpdate {
            vc.items[i].lastUpdate = when
        }
    } else {
        // Append new item at the end of the array.
        if cap(vc.items) < len(vc.items)+1 {
            // Updates should rarely happen more than once per vc in practice,
            // so append a single item.
            items := make([]itemType, len(vc.items)+1)
            copy(items, vc.items)
            vc.items = items
        } else {
            // But truncation pre-allocates full array.
            vc.items = vc.items[:len(vc.items)+1]
        }
        vc.items[len(vc.items)-1] = itemType{id, ticks, when}
    }
}


// Create a fresh empty VClock.
func New() *VClock {
    return &VClock{}
}

// Copy vc, creating a new VClock.
func (vc *VClock) Copy() *VClock {
    other := New()
    other.items = make([]itemType, len(vc.items))
    copy(other.items, vc.items)
    return other
}

// Increment the given id in vc.  The 'when' argument allows storing
// the time of the last update with the id, for posterior pruning.
// Any time unit is allowed for this purpose (shorter units will
// generate smaller byte representations, though).
func (vc *VClock) Update(id []byte, when uint64) {
    idCopy := make([]byte, len(id))
    copy(idCopy, id)
    vc.updateItem(idCopy, 1, when)
}

// Return the maximum known update timing provided to Update() with the
// given vc.
func (vc *VClock) LastUpdate() (last uint64) {
    for i := 0; i != len(vc.items); i++ {
        if vc.items[i].lastUpdate > last {
            last = vc.items[i].lastUpdate
        }
    }
    return last
}

// Compare vc to other, and return true if other is qualified as one of
// the options provided through the tests argument: Equal, Ancestor,
// Descendant, or Concurrent.  Or'ing these flags together works well.
func (vc *VClock) Compare(other *VClock, tests int) bool {
    var otherIs int

    lenVC := len(vc.items)
    lenOther := len(other.items)

    // Preliminary qualification based on length of vclock.
    if lenVC > lenOther {
        if (tests & (Ancestor | Concurrent)) == 0 {
            return false
        }
        otherIs = Ancestor
    } else if lenVC < lenOther {
        if (tests & (Descendant | Concurrent)) == 0 {
            return false
        }
        otherIs = Descendant
    } else {
        otherIs = Equal
    }

    // Compare matching items.
    lenDiff := lenOther - lenVC
    for oi := 0; oi != len(other.items); oi++ {
        if vci, found := vc.findItem(other.items[oi].id); found {
            otherTicks := other.items[oi].ticks
            vcTicks := vc.items[vci].ticks
            if otherTicks > vcTicks {
                if otherIs == Equal {
                    if (tests & Descendant) == 0 {
                        return false
                    }
                    otherIs = Descendant
                } else if otherIs == Ancestor {
                    return (tests & Concurrent) != 0
                }
            } else if otherTicks < vcTicks {
                if otherIs == Equal {
                    if (tests & Ancestor) == 0 {
                        return false
                    }
                    otherIs = Ancestor
                } else if otherIs == Descendant {
                    return (tests & Concurrent) != 0
                }
            }
        } else {
            // Other has an item which vc does not. Other must be
            // either an ancestor, or concurrent.
            if otherIs == Equal {
                // With the same length concurrent is the only choice.
                return (tests & Concurrent) != 0
            } else if lenDiff--; lenDiff < 0 {
                // Missing items. Can't be a descendant anymore.
                return (tests & Concurrent) != 0
            }
        }
    }
    return (tests & otherIs) != 0
}

// Merge the other VClock into vc, so that vc becomes a descendant of other.
// This means that every tick in other which doesn't exist in vc or which
// is smaller in vc will be copied from other to vc.
func (vc *VClock) Merge(other *VClock) {
    appends := 0
    for oi := 0; oi != len(other.items); oi++ {
        // First pass, updating old tickss and counting missing items.
        if vci, found := vc.findItem(other.items[oi].id); found {
            if vc.items[vci].ticks < other.items[oi].ticks {
                vc.items[vci].ticks = other.items[oi].ticks
            }
        } else {
            appends += 1
        }
    }

    if appends > 0 {
        // Second pass, now appending the missing ones.
        pos := len(vc.items)
        items := make([]itemType, len(vc.items)+1)
        copy(items, vc.items)
        vc.items = items
        for oi := 0; oi != len(other.items); oi++ {
            if _, found := vc.findItem(other.items[oi].id); !found {
                vc.items[pos].id = other.items[oi].id
                vc.items[pos].ticks = other.items[oi].ticks
            }
        }
    }
}

// Convert vc into bytes for storage and delivery.
func (vc *VClock) Bytes() []byte {
    if len(vc.items) == 0 {
        return []byte{}
    }
    resultSize := vc.computeBytesSize()
    result := make([]byte, resultSize)
    if vc.hasUpdateTime {
        result[0] |= 0x01 // We'll store times too.
    }
    pos := 1 // result[0] is header byte.
    for i := 0; i != len(vc.items); i++ {
        idLen := len(vc.items[i].id)
        pos += packInt(vc.items[i].ticks, result[pos:])
        if vc.hasUpdateTime {
            pos += packInt(vc.items[i].lastUpdate, result[pos:])
        }
        pos += packInt(uint64(idLen), result[pos:])
        copy(result[pos:], vc.items[i].id)
        pos += idLen
    }
    return result
}

// Read back a value generated by the Bytes() function of a
// a VClock object.
func FromBytes(value []byte) (vc *VClock, err os.Error) {
    vc = New()
    if len(value) != 0 {
        header := value[0]
        if (header &^ 0x01) != 0 {
            err = os.NewError("Bad vclock header")
            return
        }
        vc.hasUpdateTime = (header & 0x01) != 0
        pos := 1
        lastUpdate := uint64(0)
        for pos != len(value) {
            ticks, size, ok := unpackInt(value[pos:])
            pos += size
            if !ok || pos >= len(value) {
                err = os.NewError("Bad vclock data (ticks)")
                return
            }
            if vc.hasUpdateTime {
                lastUpdate, size, ok = unpackInt(value[pos:])
                pos += size
                if !ok {
                    err = os.NewError("Bad vclock data (time)")
                    return
                }
            }
            idLen, size, ok := unpackInt(value[pos:])
            pos += size
            if !ok || (pos + int(idLen)) > len(value) {
                err = os.NewError("Bad vclock data (id)")
                return
            }
            id := value[pos:pos+int(idLen)]
            pos += int(idLen)
            vc.updateItem(id, ticks, lastUpdate)
        }
    }
    return
}


func (vc *VClock) computeBytesSize() int {
    size := 0
    for i := 0; i != len(vc.items); i++ {
        size += packedIntSize(vc.items[i].ticks)
        size += packedIntSize(uint64(len(vc.items[i].id)))
        size += len(vc.items[i].id)
        if vc.hasUpdateTime {
            size += packedIntSize(vc.items[i].lastUpdate)
        }
    }
    if size > 0 {
        return size + 1 // Space for the header byte.
    }
    return 0
}


// Pack an int in big-endian format, using the 8th bit of
// each byte as a continuation flag, meaning that the next
// byte is still part of the integer.
func packInt(value uint64, out []byte) (size int) {
    size = packedIntSize(value)
    for i := size - 1; i != -1; i-- { // Big-endian.
        out[i] = uint8(value | 0x80)
        value >>= 7
    }
    out[size - 1] &^= 0x80 // Turn off the continuation bit.
    return size
}

func unpackInt(in []byte) (value uint64, size int, ok bool) {
    size = 0
    for size < len(in) && (in[size] & 0x80) != 0 {
        value |= uint64(in[size]) & 0x7f
        value <<= 7
        size += 1
    }
    if size < len(in) {
        value |= uint64(in[size])
        size += 1
        ok = true
    }
    return
}

// The size in bytes of an int packed in the format above.
func packedIntSize(value uint64) int {
    if value < 128 {
        return 1
    }
    return int(math.Ceil(math.Log2(float64(value + 1)) / 7))
}


// Specification for how to truncate a given vc VClock using the
// vc.Truncate(spec) method.
type TruncateSpec struct {
    KeepMinN int
    KeepAfter uint64
    CutAboveN int
    CutBefore uint64
}


// Truncate vc using the rules defined in the given TruncateSpec. If the
// number of entries in the vc is <= KeepMinN or all remaining entries were
// last updated after KeepAfter (compared against the 'when' value passed
// to vc.Update()), the truncation stops immediately. Otherwise, the oldest
// entries last updated prior to CutBefore or getting the vc above CutAboveN
// entries are dropped.
func (vc *VClock) Truncate(spec TruncateSpec) *VClock {
    // As an optimization, check to see if there are items to be removed
    // before going through the trouble of rebuilding the truncated VClock.
    nitems := len(vc.items)
    if nitems > spec.KeepMinN {
        for i := 0; i != nitems; i++ {
            item := &vc.items[i]
            if (spec.KeepAfter == 0 || item.lastUpdate < spec.KeepAfter) &&
               (item.lastUpdate < spec.CutBefore || nitems > spec.CutAboveN) {
                // There are items to be removed.
                return vc.actuallyTruncate(&spec)
            }
        }
    }
    // Nothing to do with vc.
    return vc.Copy()
}

func (vc *VClock) actuallyTruncate(spec *TruncateSpec) *VClock {
    items := sortItems(vc)
    truncated := New()
    truncated.items = make([]itemType, 0, len(vc.items)) // Pre-allocate all.
    for _, item := range items {
        if len(truncated.items) < spec.KeepMinN ||
           (spec.KeepAfter > 0 && item.lastUpdate > spec.KeepAfter) ||
           ((spec.CutAboveN == 0 || len(truncated.items) < spec.CutAboveN) &&
            (item.lastUpdate >= spec.CutBefore)) {
            truncated.updateItem(item.id, item.ticks, item.lastUpdate)
        }
    }
    return truncated
}

func sortItems(vc *VClock) []*itemType {
    items := make([]*itemType, len(vc.items))
    for i := 0; i != len(vc.items); i++ {
        items[i] = &vc.items[i]
    }
    sorter := itemSorter{items}
    sort.Sort(&sorter)
    return items
}

type itemSorter struct {
    items []*itemType
}

func (sorter *itemSorter) Len() int {
    return len(sorter.items)
}

func (sorter *itemSorter) Less(i, j int) bool {
    // Inverted. We want greater items first.
    return sorter.items[i].lastUpdate > sorter.items[j].lastUpdate
}

func (sorter *itemSorter) Swap(i, j int) {
    sorter.items[i], sorter.items[j] = sorter.items[j], sorter.items[i]
}
