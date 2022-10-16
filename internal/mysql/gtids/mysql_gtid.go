/*
The MIT License (MIT)

Copyright (c) 2014 siddontang

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/*
Taken from https://github.com/siddontang/go-mysql/tree/master/mysql and slightly modified
*/

package gtids

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/gofrs/uuid"
)

// Interval like MySQL GTID Interval struct, [start, stop), left closed and right open
// See MySQL rpl_gtid.h
type Interval struct {
	// The first GID of this interval.
	Start int64
	// The first GID after this interval.
	Stop int64
}

// Interval is [start, stop), but the GTID string's format is [n] or [n1-n2], closed interval
func parseInterval(str string) (i Interval, err error) {
	p := strings.Split(str, "-")
	switch len(p) {
	case 1:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		i.Stop = i.Start + 1
	case 2:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		if err != nil {
			break
		}
		i.Stop, err = strconv.ParseInt(p[1], 10, 64)
		i.Stop = i.Stop + 1
	default:
		err = fmt.Errorf("invalid interval format, must n[-n]")
	}

	if err != nil {
		return
	}

	if i.Stop <= i.Start {
		err = fmt.Errorf("invalid interval format, must n[-n] and the end must >= start")
	}

	return
}

func (i Interval) String() string {
	if i.Stop == i.Start+1 {
		return fmt.Sprintf("%d", i.Start)
	}
	return fmt.Sprintf("%d-%d", i.Start, i.Stop-1)
}

// IntervalSlice is Slice of Interval's
type IntervalSlice []Interval

func (s IntervalSlice) Len() int {
	return len(s)
}

func (s IntervalSlice) Less(i, j int) bool {
	if s[i].Start < s[j].Start {
		return true
	} else if s[i].Start > s[j].Start {
		return false
	} else {
		return s[i].Stop < s[j].Stop
	}
}

// Swap is to conform sort.Interface
func (s IntervalSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort is to conform sort.Interface
func (s IntervalSlice) Sort() {
	sort.Sort(s)
}

// Normalize makes interval set disjoint
func (s IntervalSlice) Normalize() IntervalSlice {
	var n IntervalSlice
	if len(s) == 0 {
		return n
	}

	s.Sort()

	n = append(n, s[0])

	for i := 1; i < len(s); i++ {
		last := n[len(n)-1]
		if s[i].Start > last.Stop {
			n = append(n, s[i])
			continue
		} else {
			stop := s[i].Stop
			if last.Stop > stop {
				stop = last.Stop
			}
			n[len(n)-1] = Interval{last.Start, stop}
		}
	}

	return n
}

// Contain returns true if sub in s
func (s IntervalSlice) Contain(sub IntervalSlice) bool {
	j := 0
	for i := 0; i < len(sub); i++ {
		for ; j < len(s); j++ {
			if sub[i].Start > s[j].Stop {
				continue
			} else {
				break
			}
		}
		if j == len(s) {
			return false
		}

		if sub[i].Start < s[j].Start || sub[i].Stop > s[j].Stop {
			return false
		}
	}

	return true
}

// Equal returns true if two IntervalSlice are the same
func (s IntervalSlice) Equal(o IntervalSlice) bool {
	if len(s) != len(o) {
		return false
	}

	for i := 0; i < len(s); i++ {
		if s[i].Start != o[i].Start || s[i].Stop != o[i].Stop {
			return false
		}
	}

	return true
}

// IsComparable returns true is either of IntervalSlices contains another one
func (s IntervalSlice) IsComparable(o IntervalSlice) bool {
	return s.Contain(o) || o.Contain(s)
}

// Compare returns 1, -1 or 0 if s greater(contains), smaller(contained by) or equal(or incomparable) to o
func (s IntervalSlice) Compare(o IntervalSlice) int {
	if s.Contain(o) {
		return 1
	} else if o.Contain(s) {
		return -1
	} else {
		return 0
	}
}

// UUIDSet represen set of transaction applied on particular MySQL node
type UUIDSet struct {
	SID uuid.UUID

	Intervals IntervalSlice
}

// ParseUUIDSet parses UUIDSet from string
func ParseUUIDSet(str string) (*UUIDSet, error) {
	str = strings.TrimSpace(str)
	sep := strings.Split(str, ":")
	if len(sep) < 2 {
		return nil, fmt.Errorf("invalid GTID format, must UUID:interval[:interval]")
	}

	var err error
	s := new(UUIDSet)
	if s.SID, err = uuid.FromString(sep[0]); err != nil {
		return nil, err
	}

	// Handle interval
	for i := 1; i < len(sep); i++ {
		if in, err := parseInterval(sep[i]); err == nil {
			s.Intervals = append(s.Intervals, in)
		} else {
			return nil, err
		}
	}

	s.Intervals = s.Intervals.Normalize()

	return s, nil
}

// NewUUIDSet constructs UUIDSet from UUID and set of Intervals
func NewUUIDSet(sid uuid.UUID, in ...Interval) *UUIDSet {
	s := new(UUIDSet)
	s.SID = sid

	s.Intervals = in
	s.Intervals = s.Intervals.Normalize()

	return s
}

// Contain return true if s and sub has the same UUID and sub in s
func (s *UUIDSet) Contain(sub *UUIDSet) bool {
	if !bytes.Equal(s.SID.Bytes(), sub.SID.Bytes()) {
		return false
	}

	return s.Intervals.Contain(sub.Intervals)
}

// IsComparable returns true is either of UUIDSet contains another one
func (s *UUIDSet) IsComparable(o *UUIDSet) bool {
	return s.Contain(o) || o.Contain(s)
}

// Bytes returns text representation of UUIDSet as []byte
func (s *UUIDSet) Bytes() []byte {
	var buf bytes.Buffer

	buf.WriteString(s.SID.String())

	for _, i := range s.Intervals {
		buf.WriteString(":")
		buf.WriteString(i.String())
	}

	return buf.Bytes()
}

// AddInterval adds IntervalSlice to UUIDSet and then normalizes it
func (s *UUIDSet) AddInterval(in IntervalSlice) {
	s.Intervals = append(s.Intervals, in...)
	s.Intervals = s.Intervals.Normalize()
}

// String returns text representation of UUIDSet
func (s *UUIDSet) String() string {
	return string(s.Bytes())
}

func (s *UUIDSet) encode(w io.Writer) {
	_, _ = w.Write(s.SID.Bytes())
	n := int64(len(s.Intervals))

	_ = binary.Write(w, binary.LittleEndian, n)

	for _, i := range s.Intervals {
		_ = binary.Write(w, binary.LittleEndian, i.Start)
		_ = binary.Write(w, binary.LittleEndian, i.Stop)
	}
}

// Encode returns binary representation of UUIDSet
func (s *UUIDSet) Encode() []byte {
	var buf bytes.Buffer

	s.encode(&buf)

	return buf.Bytes()
}

func (s *UUIDSet) decode(data []byte) (int, error) {
	if len(data) < 24 {
		return 0, fmt.Errorf("invalid uuid set buffer, less 24")
	}

	pos := 0
	var err error
	if s.SID, err = uuid.FromBytes(data[0:16]); err != nil {
		return 0, err
	}
	pos += 16

	n := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	pos += 8
	if len(data) < int(16*n)+pos {
		return 0, fmt.Errorf("invalid uuid set buffer, must %d, but %d", pos+int(16*n), len(data))
	}

	s.Intervals = make([]Interval, 0, n)

	var in Interval
	for i := int64(0); i < n; i++ {
		in.Start = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		in.Stop = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		s.Intervals = append(s.Intervals, in)
	}

	return pos, nil
}

// Decode parses UUIDSet from binary representation
func (s *UUIDSet) Decode(data []byte) error {
	n, err := s.decode(data)
	if n != len(data) {
		return fmt.Errorf("invalid uuid set buffer, must %d, but %d", n, len(data))
	}
	return err
}

// Clone returns normalized copy of UUIDSet
func (s *UUIDSet) Clone() *UUIDSet {
	clone := new(UUIDSet)

	clone.SID, _ = uuid.FromString(s.SID.String())
	clone.Intervals = s.Intervals.Normalize()

	return clone
}

// MysqlGTIDSet represents set of transactions applied on MySQL cluster
type MysqlGTIDSet struct {
	Sets map[string]*UUIDSet
}

// ParseMysqlGTIDSet parses MysqlGTIDSet from string
func ParseMysqlGTIDSet(str string) (GTIDSet, error) {
	s := new(MysqlGTIDSet)
	s.Sets = make(map[string]*UUIDSet)
	if str == "" {
		return s, nil
	}

	sp := strings.Split(str, ",")

	//todo, handle redundant same uuid
	for i := 0; i < len(sp); i++ {
		if set, err := ParseUUIDSet(sp[i]); err == nil {
			s.AddSet(set)
		} else {
			return nil, err
		}

	}
	return s, nil
}

// DecodeMysqlGTIDSet decodes MysqlGTIDSet from binary protocol representation
func DecodeMysqlGTIDSet(data []byte) (*MysqlGTIDSet, error) {
	s := new(MysqlGTIDSet)

	if len(data) < 8 {
		return nil, fmt.Errorf("invalid gtid set buffer, less 4")
	}

	n := int(binary.LittleEndian.Uint64(data))
	s.Sets = make(map[string]*UUIDSet, n)

	pos := 8

	for i := 0; i < n; i++ {
		set := new(UUIDSet)
		if n, err := set.decode(data[pos:]); err == nil {
			pos += n
			s.AddSet(set)
		} else {
			return nil, err
		}
	}
	return s, nil
}

// AddSet adds new UUIDSet to MysqlGTIDSet
func (s *MysqlGTIDSet) AddSet(set *UUIDSet) {
	if set == nil {
		return
	}
	sid := set.SID.String()
	o, ok := s.Sets[sid]
	if ok {
		o.AddInterval(set.Intervals)
	} else {
		s.Sets[sid] = set
	}
}

// Update adds GTID set from string to MysqlGTIDSet
func (s *MysqlGTIDSet) Update(GTIDStr string) error {
	gtidSet, err := ParseMysqlGTIDSet(GTIDStr)
	if err != nil {
		return err
	}
	for _, uuidSet := range gtidSet.(*MysqlGTIDSet).Sets {
		s.AddSet(uuidSet)
	}
	return nil
}

// Contain return true if MysqlGTIDSet contains all the UUIDs from other GTIDSet
// and for each UUID contains all their intervals
func (s *MysqlGTIDSet) Contain(o GTIDSet) bool {
	sub, ok := o.(*MysqlGTIDSet)
	if !ok {
		return false
	}

	for key, set := range sub.Sets {
		o, ok := s.Sets[key]
		if !ok {
			return false
		}

		if !o.Contain(set) {
			return false
		}
	}

	return true
}

// IsComparable returns true is either of MysqlGTIDSet contains another one
func (s *MysqlGTIDSet) IsComparable(o *MysqlGTIDSet) bool {
	return s.Contain(o) || o.Contain(s)
}

// Equal returns true if two GTIDSets are the same
func (s *MysqlGTIDSet) Equal(o GTIDSet) bool {
	sub, ok := o.(*MysqlGTIDSet)
	if !ok {
		return false
	}

	if len(sub.Sets) != len(s.Sets) {
		return false
	}

	for key, set := range sub.Sets {
		o, ok := s.Sets[key]
		if !ok {
			return false
		}

		if !o.Intervals.Equal(set.Intervals) {
			return false
		}
	}

	return true

}

func (s *MysqlGTIDSet) String() string {
	var buf bytes.Buffer
	sep := ""
	for _, set := range s.Sets {
		buf.WriteString(sep)
		buf.WriteString(set.String())
		sep = ","
	}

	return buf.String()
}

// Encode returns binary representation of MysqlGTIDSet
func (s *MysqlGTIDSet) Encode() []byte {
	var buf bytes.Buffer

	_ = binary.Write(&buf, binary.LittleEndian, uint64(len(s.Sets)))

	for i := range s.Sets {
		s.Sets[i].encode(&buf)
	}

	return buf.Bytes()
}

// Clone returns normalized copy of MysqlGTIDSet
func (s *MysqlGTIDSet) Clone() GTIDSet {
	clone := &MysqlGTIDSet{
		Sets: make(map[string]*UUIDSet),
	}
	for sid, uuidSet := range s.Sets {
		clone.Sets[sid] = uuidSet.Clone()
	}

	return clone
}
