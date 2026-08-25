package fakestorage

import (
	"fmt"
	"unicode/utf8"
)

// invalidMatchGlobMessage returns the error message that the GCS JSON API
// returns for an invalid matchGlob pattern, or "" when the pattern is valid.
// The messages are returned verbatim, so callers can put them on the wire.
//
// The rules and messages come from probing live GCS (JSON API, 2026-08-24/25):
//
//   - '\' escapes the next character anywhere; a trailing '\' is accepted.
//   - ']' or '}' without a matching opener is an error.
//   - Braces must balance. An unclosed '{' reports the innermost one.
//   - A character class must be closed and must have at least one member.
//     A leading '!' or '^' negates the class and does not count as a member.
//   - Inside a class, '[' and '{' must be escaped. So must '}', and ','
//     while a brace expansion is open; unescaped, they abort the class.
//   - Ranges inside a class must be ascending and compare by code point.
//   - Patterns need not be valid UTF-8. Live GCS decodes invalid bytes to
//     U+FFFD, as DecodeRuneInString does here: [\xFF-\xFE] is accepted as
//     an equal-endpoint range, and a range error renders the replacement
//     character in the message.
//
// Indexes in the messages are byte offsets, as on live GCS.
func invalidMatchGlobMessage(pattern string) string {
	var braceStack []int
	for i := 0; i < len(pattern); {
		r, size := utf8.DecodeRuneInString(pattern[i:])
		switch r {
		case '\\':
			i += size
			if i < len(pattern) {
				_, escSize := utf8.DecodeRuneInString(pattern[i:])
				i += escSize
			}
			continue
		case '[':
			next, msg := scanMatchGlobClass(pattern, i, len(braceStack) > 0)
			if msg != "" {
				return msg
			}
			i = next
			continue
		case ']':
			return fmt.Sprintf("Glob pattern had closing character class ']' at index %d without any previous opening '['. Either add an opening '[' or use '\\]' to match ']'.", i)
		case '{':
			braceStack = append(braceStack, i)
		case '}':
			if len(braceStack) == 0 {
				return bareClosingBraceMessage(i)
			}
			braceStack = braceStack[:len(braceStack)-1]
		}
		i += size
	}
	if len(braceStack) > 0 {
		return fmt.Sprintf("Glob pattern brace expansion opening '{' at index %d has no closing '}'. Use '\\{' to match a literal '{'.", braceStack[len(braceStack)-1])
	}
	return ""
}

// scanMatchGlobClass validates the character class that opens at
// pattern[start] and returns the byte offset just past its closing ']'.
// On an invalid class it returns the GCS error message instead.
// inBraces reports whether a brace expansion is open at the class: GCS
// aborts a class on the characters that would end a brace arm (',' and
// '}') only in that context.
func scanMatchGlobClass(pattern string, start int, inBraces bool) (int, string) {
	unclosed := func() string {
		return fmt.Sprintf("Glob pattern character class has no closing ']' for opening '[' at index %d. Either add a closing ']' or use '\\[' to match a literal '['.", start)
	}

	members := 0
	rangeLo := rune(-1)  // last member, candidate range start; -1 when none
	pendingDash := false // saw "<member>-" and the range end is still open
	member := func(r rune) string {
		if pendingDash {
			if rangeLo > r {
				return fmt.Sprintf("Invalid range '%c-%c' in glob pattern character class.", rangeLo, r)
			}
			pendingDash = false
			rangeLo = -1
		} else {
			rangeLo = r
		}
		members++
		return ""
	}

	i := start + 1
	// '!' and '^' are ASCII, so the byte test needs no rune decoding.
	if i < len(pattern) && (pattern[i] == '!' || pattern[i] == '^') {
		i++
	}
	for i < len(pattern) {
		r, size := utf8.DecodeRuneInString(pattern[i:])
		switch r {
		case '\\':
			i += size
			if i >= len(pattern) {
				return 0, unclosed()
			}
			esc, escSize := utf8.DecodeRuneInString(pattern[i:])
			if msg := member(esc); msg != "" {
				return 0, msg
			}
			i += escSize
			continue
		case ']':
			if members == 0 {
				return 0, fmt.Sprintf("Glob pattern character class at index %d must have at least one character in it.", start)
			}
			return i + size, ""
		case '[':
			return 0, fmt.Sprintf("Glob pattern character class does not support the unescaped '[' at index %d. Use '\\[' to match '['.", i)
		case '{':
			return 0, unclosed()
		case '}':
			if !inBraces {
				return 0, bareClosingBraceMessage(i)
			}
			return 0, unclosed()
		case ',':
			if inBraces {
				return 0, unclosed()
			}
			if msg := member(r); msg != "" {
				return 0, msg
			}
		case '-':
			if rangeLo >= 0 && !pendingDash {
				pendingDash = true
			} else if msg := member(r); msg != "" {
				return 0, msg
			}
		default:
			if msg := member(r); msg != "" {
				return 0, msg
			}
		}
		i += size
	}
	return 0, unclosed()
}

func bareClosingBraceMessage(index int) string {
	return fmt.Sprintf("Glob pattern brace expansion closing '}' at index %d has no previous opening '{'. Use '\\}' to match a literal '}'.", index)
}
