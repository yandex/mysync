package gtids

func ParseGtidSet(gtidset string) GTIDSet {
	parsed, err := ParseGTIDSet(MySQLFlavor, gtidset)
	if err != nil {
		panic(err)
	}
	return parsed
}
