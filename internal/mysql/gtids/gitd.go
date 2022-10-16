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
Taken from https://github.com/siddontang/go-mysql/tree/master/mysql and slightly changed
*/

package gtids

import "fmt"

const (
	// MySQLFlavor denotes MySQL syntax for GTIDSet string
	MySQLFlavor = "mysql"
	// MariaDBFlavor denotes MariaDB syntax for GTIDSet string
	MariaDBFlavor = "mariadb"
)

// GTIDSet represents set of transactions in MYSQL
type GTIDSet interface {
	String() string

	// Encode GTID set into binary format used in binlog dump commands
	Encode() []byte

	Equal(o GTIDSet) bool

	Contain(o GTIDSet) bool

	Update(GTIDStr string) error

	Clone() GTIDSet
}

// ParseGTIDSet parses string with GTIDSet into structure according to MySQL/MariaDB flavor
func ParseGTIDSet(flavor string, s string) (GTIDSet, error) {
	switch flavor {
	case MySQLFlavor:
		return ParseMysqlGTIDSet(s)
	case MariaDBFlavor:
		return ParseMariadbGTIDSet(s)
	default:
		return nil, fmt.Errorf("invalid flavor %s", flavor)
	}
}
