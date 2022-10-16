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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMysqlGTIDInterval(t *testing.T) {
	i, err := parseInterval("1-2")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 3}, i)

	i, err = parseInterval("1")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 2}, i)

	i, err = parseInterval("1-1")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 2}, i)

	i, err = parseInterval("1-2")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 3}, i)
}

func TestMysqlGTIDIntervalSlice(t *testing.T) {
	i := IntervalSlice{Interval{1, 2}, Interval{2, 4}, Interval{2, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 2}, Interval{2, 3}, Interval{2, 4}}, i)
	n := i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 4}}, n)

	i = IntervalSlice{Interval{1, 2}, Interval{3, 5}, Interval{1, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 2}, Interval{1, 3}, Interval{3, 5}}, i)
	n = i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 5}}, n)

	i = IntervalSlice{Interval{1, 2}, Interval{4, 5}, Interval{1, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 2}, Interval{1, 3}, Interval{4, 5}}, i)
	n = i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 3}, Interval{4, 5}}, n)

	i = IntervalSlice{Interval{1, 4}, Interval{2, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 4}, Interval{2, 3}}, i)
	n = i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 4}}, n)

	n1 := IntervalSlice{Interval{1, 3}, Interval{4, 5}}
	n2 := IntervalSlice{Interval{1, 2}}

	require.True(t, n1.Contain(n2))
	require.False(t, n2.Contain(n1))

	n1 = IntervalSlice{Interval{1, 3}, Interval{4, 5}}
	n2 = IntervalSlice{Interval{1, 6}}

	require.False(t, n1.Contain(n2))
	require.True(t, n2.Contain(n1))

	n1 = IntervalSlice{Interval{1, 2}}
	n2 = IntervalSlice{Interval{1, 2}}

	require.True(t, n1.Contain(n2))
	require.True(t, n2.Contain(n1))

	n1 = IntervalSlice{Interval{1, 5}}
	n2 = IntervalSlice{Interval{3, 7}}

	require.False(t, n1.Contain(n2))
	require.False(t, n2.Contain(n1))

	n1 = IntervalSlice{Interval{1, 3}, Interval{4, 5}}
	require.True(t, n1.Contain(n1))
}

func TestMysqlGTIDCodec(t *testing.T) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.NoError(t, err)

	require.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", us.String())

	buf := us.Encode()
	err = us.Decode(buf)
	require.NoError(t, err)

	gs, err := ParseMysqlGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2,de278ad0-2106-11e4-9f8e-6edd0ca20948:1-2")
	require.NoError(t, err)

	buf = gs.Encode()
	o, err := DecodeMysqlGTIDSet(buf)
	require.NoError(t, err)
	require.Equal(t, o, gs)
}

func TestMysqlUpdate(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	err = g1.Update("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58")

	require.NoError(t, err)
	require.Equal(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58", strings.ToUpper(g1.String()))

	g1, err = ParseMysqlGTIDSet(`
		519CE70F-A893-11E9-A95A-B32DC65A7026:1-1154661,
		5C9CA52B-9F11-11E9-8EAF-3381EC1CC790:1-244,
		802D69FD-A3B6-11E9-B1EA-50BAB55BA838:1-1221371,
		F2B50559-A891-11E9-B646-884FF0CA2043:1-479261
	`)
	require.NoError(t, err)

	err = g1.Update(`
		802D69FD-A3B6-11E9-B1EA-50BAB55BA838:1221110-1221371,
		F2B50559-A891-11E9-B646-884FF0CA2043:478509-479266
	`)
	require.NoError(t, err)

	g2, err := ParseMysqlGTIDSet(`
		519CE70F-A893-11E9-A95A-B32DC65A7026:1-1154661,
		5C9CA52B-9F11-11E9-8EAF-3381EC1CC790:1-244,
		802D69FD-A3B6-11E9-B1EA-50BAB55BA838:1-1221371,
		F2B50559-A891-11E9-B646-884FF0CA2043:1-479266
	`)
	require.NoError(t, err)
	require.True(t, g2.Equal(g1))
}

func TestMysqlGTIDContain(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	require.NoError(t, err)

	g2, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	require.True(t, g2.Contain(g1))
	require.False(t, g1.Contain(g2))

	g1, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:15-40")
	require.NoError(t, err)

	g2, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	require.False(t, g2.Contain(g1))
	require.False(t, g1.Contain(g2))

	g1, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	g2, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57,79A7AD13-5A06-42C0-B86D-39263D04EF95:1-5")
	require.NoError(t, err)

	require.True(t, g2.Contain(g1))
	require.False(t, g1.Contain(g2))
	require.False(t, g2.Equal(g1))
	require.False(t, g1.Equal(g2))

	g1, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-60")
	require.NoError(t, err)

	g2, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57,79A7AD13-5A06-42C0-B86D-39263D04EF95:1-5")
	require.NoError(t, err)

	require.False(t, g2.Contain(g1))
	require.False(t, g1.Contain(g2))
	require.False(t, g2.Equal(g1))
	require.False(t, g1.Equal(g2))

	g1, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57,85FCB455-EE80-4EB9-93C7-1E6DE587C02D:1-5")
	require.NoError(t, err)

	g2, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57,79A7AD13-5A06-42C0-B86D-39263D04EF95:1-5")
	require.NoError(t, err)

	require.False(t, g2.Contain(g1))
	require.False(t, g1.Contain(g2))
	require.False(t, g2.Equal(g1))
	require.False(t, g1.Equal(g2))

	g1, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57,79A7AD13-5A06-42C0-B86D-39263D04EF95:1-5")
	require.NoError(t, err)

	g2, err = ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57,79A7AD13-5A06-42C0-B86D-39263D04EF95:1-5")
	require.NoError(t, err)

	require.True(t, g2.Contain(g1))
	require.True(t, g1.Contain(g2))
	require.True(t, g2.Equal(g1))
	require.True(t, g1.Equal(g2))
}
