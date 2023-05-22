package dcs

import "strings"

func buildFullPath(paths ...string) string {
	b := strings.Builder{}
	length := 1
	for _, v := range paths {
		length += len(v)
	}
	b.Grow(length)
	for _, v := range paths {
		buildPathPart(&b, v)
	}
	if b.Len() == 0 {
		b.WriteRune(sepRune)
	}
	return b.String()
}

func buildPathPart(b *strings.Builder, s string) {
	prev := sepRune
	for _, v := range s {
		if v != sepRune {
			if prev == sepRune {
				b.WriteRune(sepRune)
			}
			b.WriteRune(v)
		}
		prev = v
	}
}
