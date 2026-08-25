package fakestorage

import "testing"

// The patterns and messages in this table were captured from live GCS
// (JSON API, 2026-08-24/25). want is "" for patterns that GCS accepts.
func TestInvalidMatchGlobMessage(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{
			"empty pattern",
			"",
			"",
		},
		{
			"valid baseline",
			"test/*.jpg",
			"",
		},
		{
			"unclosed char class",
			"test/[abc.jpg",
			"Glob pattern character class has no closing ']' for opening '[' at index 5. Either add a closing ']' or use '\\[' to match a literal '['.",
		},
		{
			"lone open bracket at end",
			"test/[",
			"Glob pattern character class has no closing ']' for opening '[' at index 5. Either add a closing ']' or use '\\[' to match a literal '['.",
		},
		{
			"unclosed brace",
			"test/{a,b.jpg",
			"Glob pattern brace expansion opening '{' at index 5 has no closing '}'. Use '\\{' to match a literal '{'.",
		},
		{
			"lone open brace",
			"test/{",
			"Glob pattern brace expansion opening '{' at index 5 has no closing '}'. Use '\\{' to match a literal '{'.",
		},
		{
			"nested unclosed brace reports the innermost",
			"test/{a,{b",
			"Glob pattern brace expansion opening '{' at index 8 has no closing '}'. Use '\\{' to match a literal '{'.",
		},
		{
			"empty char class",
			"test/[].jpg",
			"Glob pattern character class at index 5 must have at least one character in it.",
		},
		{
			"negated empty class",
			"test/[!].jpg",
			"Glob pattern character class at index 5 must have at least one character in it.",
		},
		{
			"caret-negated empty class",
			"test/[^].jpg",
			"Glob pattern character class at index 5 must have at least one character in it.",
		},
		{
			"no POSIX first-bracket-is-literal rule",
			"test/[]a].jpg",
			"Glob pattern character class at index 5 must have at least one character in it.",
		},
		{
			"bare closing bracket",
			"test/a].jpg",
			"Glob pattern had closing character class ']' at index 6 without any previous opening '['. Either add an opening '[' or use '\\]' to match ']'.",
		},
		{
			"bare closing brace",
			"test/a}.jpg",
			"Glob pattern brace expansion closing '}' at index 6 has no previous opening '{'. Use '\\}' to match a literal '}'.",
		},
		{
			"reversed range in class",
			"test/[z-a].jpg",
			"Invalid range 'z-a' in glob pattern character class.",
		},
		{
			"unescaped open bracket inside class",
			"test/[[].jpg",
			"Glob pattern character class does not support the unescaped '[' at index 6. Use '\\[' to match '['.",
		},
		{
			"unescaped open brace inside class",
			"test/[{].jpg",
			"Glob pattern character class has no closing ']' for opening '[' at index 5. Either add a closing ']' or use '\\[' to match a literal '['.",
		},
		{
			"full brace expansion inside class",
			"test/[{a}].jpg",
			"Glob pattern character class has no closing ']' for opening '[' at index 5. Either add a closing ']' or use '\\[' to match a literal '['.",
		},
		{
			"closing brace inside class, no brace open",
			"test/[}].jpg",
			"Glob pattern brace expansion closing '}' at index 6 has no previous opening '{'. Use '\\}' to match a literal '}'.",
		},
		{
			"unclosed class then closing brace, no brace open",
			"test/[ab}.jpg",
			"Glob pattern brace expansion closing '}' at index 8 has no previous opening '{'. Use '\\}' to match a literal '}'.",
		},
		{
			"closing brace inside class aborts the class when a brace is open",
			"test/{a,[}]}.jpg",
			"Glob pattern character class has no closing ']' for opening '[' at index 8. Either add a closing ']' or use '\\[' to match a literal '['.",
		},
		{
			"comma inside class aborts the class when a brace is open",
			"test/{[a,b]}.jpg",
			"Glob pattern character class has no closing ']' for opening '[' at index 6. Either add a closing ']' or use '\\[' to match a literal '['.",
		},
		{
			"index is a byte offset",
			"é[abc",
			"Glob pattern character class has no closing ']' for opening '[' at index 2. Either add a closing ']' or use '\\[' to match a literal '['.",
		},
		{
			"trailing backslash is accepted",
			`test/a\`,
			"",
		},
		{
			"nested braces",
			"test/{a,{b,c}}.jpg",
			"",
		},
		{
			"slash inside class",
			"test/[a/b].jpg",
			"",
		},
		{
			"escape of ordinary char",
			`test/\a.jpg`,
			"",
		},
		{
			"dash at class end",
			"test/[a-].jpg",
			"",
		},
		{
			"dash at class start",
			"test/[-a].jpg",
			"",
		},
		{
			"escaped closing bracket inside class",
			`test/[a\]b].jpg`,
			"",
		},
		{
			"escaped opening brace needs no close",
			`test/\{a.jpg`,
			"",
		},
		{
			"range with escaped endpoint",
			`test/[\a-b].jpg`,
			"",
		},
		{
			"well-formed class in brace arm",
			"test/{a,[b]}.jpg",
			"",
		},
		{
			"comma in class outside braces",
			"test/[a,b].jpg",
			"",
		},
		{
			"comma outside braces",
			"test/a,b.jpg",
			"",
		},
		{
			"class of single escaped bracket",
			`test/[\]].jpg`,
			"",
		},
		{
			"invalid UTF-8 byte is accepted",
			"test/\xff.jpg",
			"",
		},
		{
			"invalid UTF-8 byte inside class",
			"test/[\xff].jpg",
			"",
		},
		{
			"range between invalid bytes is accepted, both decode to U+FFFD",
			"test/[\xff-\xfe].jpg",
			"",
		},
		{
			"unpaired surrogate encoding is accepted",
			"test/\xed\xa0\x80.jpg",
			"",
		},
		{
			"lone continuation byte is accepted",
			"test/\xb1.jpg",
			"",
		},
		{
			"ascending multibyte range",
			"test/[α-ω].jpg",
			"",
		},
		{
			"reversed multibyte range compares by code point",
			"test/[ω-α].jpg",
			"Invalid range 'ω-α' in glob pattern character class.",
		},
		{
			"range from invalid byte to ASCII is reversed after replacement",
			"test/[\xff-a].jpg",
			"Invalid range '�-a' in glob pattern character class.",
		},
		{
			"range from ASCII to invalid byte ascends after replacement",
			"test/[a-\xff].jpg",
			"",
		},
		{
			"class of single escaped multibyte member",
			`test/[\é].jpg`,
			"",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := invalidMatchGlobMessage(test.pattern); got != test.want {
				t.Errorf("invalidMatchGlobMessage(%q)\ngot:  %q\nwant: %q", test.pattern, got, test.want)
			}
		})
	}
}
