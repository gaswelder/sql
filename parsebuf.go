package main

import (
	"strings"
)

// Parsebuf is a string container with utility methods for writing hand-crafred
// parsers.
type Parsebuf struct {
	pos int
	str string
}

// NewParsebuf returns a new parsebuf.
func NewParsebuf(s string) *Parsebuf {
	return &Parsebuf{0, s}
}

// More returns true if there are more characters to read.
func (b *Parsebuf) More() bool {
	return b.pos < len(b.str)
}

// Get reads one character. Returns empty string if there's no more characters.
func (b *Parsebuf) Get() string {
	if !b.More() {
		return ""
	}
	s := b.str[b.pos : b.pos+1]
	b.pos++
	return s
}

// Peek return what Get would return, without reading it.
func (b *Parsebuf) Peek() string {
	if !b.More() {
		return ""
	}
	return b.str[b.pos : b.pos+1]
}

// Set reads and returns a sequence of characters from the given set.
func (b *Parsebuf) Set(allowed string) string {
	s := strings.Builder{}
	for b.More() && strings.Contains(allowed, b.Peek()) {
		s.WriteString(b.Get())
	}
	return s.String()
}

// Space reads a sequence of conventional spaces.
func (b *Parsebuf) Space() string {
	return b.Set(" \n\t")
}

// Literal reads the given string, case-insensitive, and return true on success.
// Returns false if a matching literal doesn't follow.
func (b *Parsebuf) Literal(literal string) bool {
	if !strings.HasPrefix(strings.ToLower(b.str[b.pos:]), strings.ToLower(literal)) {
		return false
	}
	b.pos += len(literal)
	return true
}

// Rest returns the unconsumed part of the buffer's string.
func (b *Parsebuf) Rest() string {
	return b.str[b.pos:]
}
