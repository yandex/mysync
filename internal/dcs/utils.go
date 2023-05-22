package dcs

import "strings"

func buildFullPath(prefix, path string) string {
	b := strings.Builder{}
	b.Grow(1 + len(prefix) + len(path))
	buildPathPart(&b, prefix)
	buildPathPart(&b, path)
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
