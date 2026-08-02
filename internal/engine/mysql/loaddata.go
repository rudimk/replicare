package mysql

import (
	"bufio"
	"io"
)

// LOAD DATA text serialization (mysql-plan §0.1, Momus B6). The initial-copy and
// apply transports move values as raw bytes through a LOAD DATA LOCAL INFILE
// stream. This file is the byte-faithful serializer: it renders each row as
// tab-separated, newline-terminated fields with a COMPLETE escape set so binary
// and text data round-trip byte-for-byte, and NULL is the unambiguous \N
// sentinel. It pairs with the load clause:
//
//	FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n'
//
// Escape set (each maps a raw byte to a MySQL LOAD DATA escape sequence so the
// reader reconstructs the exact byte): NUL, backspace, tab, newline, CR, Ctrl-Z,
// and the escape char itself. A literal 'N' is written verbatim; only the NULL
// sentinel is the two bytes "\N", and a data byte-pair 0x5C 0x4E ("\N") is
// written as "\\N" (escaped backslash + N) so it is never mistaken for NULL.
const (
	fieldSep  = '\t'
	rowTerm   = '\n'
	escByte   = '\\'
	nullField = "\\N"
)

// escapeInto writes v to w with LOAD DATA escaping. v is nil for SQL NULL.
func escapeInto(w *bufio.Writer, v []byte) {
	for _, b := range v {
		switch b {
		case 0x00:
			w.WriteByte(escByte)
			w.WriteByte('0')
		case 0x08:
			w.WriteByte(escByte)
			w.WriteByte('b')
		case '\t':
			w.WriteByte(escByte)
			w.WriteByte('t')
		case '\n':
			w.WriteByte(escByte)
			w.WriteByte('n')
		case '\r':
			w.WriteByte(escByte)
			w.WriteByte('r')
		case 0x1A:
			w.WriteByte(escByte)
			w.WriteByte('Z')
		case escByte:
			w.WriteByte(escByte)
			w.WriteByte(escByte)
		default:
			w.WriteByte(b)
		}
	}
}

// writeRow serializes one row (a slice of values, nil = NULL) as a LOAD DATA
// line. It does not flush.
func writeRow(w *bufio.Writer, row [][]byte) {
	for i, v := range row {
		if i > 0 {
			w.WriteByte(fieldSep)
		}
		if v == nil {
			w.WriteString(nullField)
			continue
		}
		escapeInto(w, v)
	}
	w.WriteByte(rowTerm)
}

// loadDataClause is the fixed FIELDS/LINES clause matching the serializer.
const loadDataClause = "FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'"

// newRowWriter wraps w in a buffered writer sized for streaming copy.
func newRowWriter(w io.Writer) *bufio.Writer { return bufio.NewWriterSize(w, 64*1024) }
