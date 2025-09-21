package query

import (
	"strings"
	"unicode"
)

// Lexer tokenizes query strings
type Lexer struct {
	input    string
	position int
	line     int
	column   int
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		line:   1,
		column: 1,
	}
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.position >= len(l.input) {
		return Token{Type: TokenEOF, Line: l.line, Column: l.column}
	}

	ch := l.input[l.position]
	startColumn := l.column

	switch ch {
	case '(':
		l.advance()
		return Token{Type: TokenLeftParen, Value: "(", Line: l.line, Column: startColumn}
	case ')':
		l.advance()
		return Token{Type: TokenRightParen, Value: ")", Line: l.line, Column: startColumn}
	case ',':
		l.advance()
		return Token{Type: TokenComma, Value: ",", Line: l.line, Column: startColumn}
	case '.':
		l.advance()
		return Token{Type: TokenDot, Value: ".", Line: l.line, Column: startColumn}
	case '\'', '"':
		return l.readString()
	case '=', '!', '<', '>':
		return l.readComparison()
	default:
		if unicode.IsLetter(rune(ch)) || ch == '_' {
			return l.readIdentifier()
		}
		if unicode.IsDigit(rune(ch)) {
			return l.readNumber()
		}
		// Unknown character, skip it
		l.advance()
		return l.NextToken()
	}
}

func (l *Lexer) advance() {
	if l.position < len(l.input) && l.input[l.position] == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
	l.position++
}

func (l *Lexer) skipWhitespace() {
	for l.position < len(l.input) && unicode.IsSpace(rune(l.input[l.position])) {
		l.advance()
	}
}

func (l *Lexer) readString() Token {
	quote := l.input[l.position]
	startColumn := l.column
	l.advance() // Skip opening quote

	var value strings.Builder
	for l.position < len(l.input) && l.input[l.position] != quote {
		if l.input[l.position] == '\\' && l.position+1 < len(l.input) {
			l.advance() // Skip backslash
			switch l.input[l.position] {
			case 'n':
				value.WriteByte('\n')
			case 't':
				value.WriteByte('\t')
			case 'r':
				value.WriteByte('\r')
			case '\\':
				value.WriteByte('\\')
			case '\'':
				value.WriteByte('\'')
			case '"':
				value.WriteByte('"')
			default:
				value.WriteByte(l.input[l.position])
			}
		} else {
			value.WriteByte(l.input[l.position])
		}
		l.advance()
	}

	if l.position < len(l.input) {
		l.advance() // Skip closing quote
	}

	return Token{Type: TokenString, Value: value.String(), Line: l.line, Column: startColumn}
}

func (l *Lexer) readIdentifier() Token {
	startColumn := l.column
	var value strings.Builder

	for l.position < len(l.input) && (unicode.IsLetter(rune(l.input[l.position])) ||
		unicode.IsDigit(rune(l.input[l.position])) || l.input[l.position] == '_') {
		value.WriteByte(l.input[l.position])
		l.advance()
	}

	tokenValue := value.String()
	tokenType := TokenIdentifier

	// Check for keywords
	switch strings.ToUpper(tokenValue) {
	case "AND":
		tokenType = TokenLogical
	case "OR":
		tokenType = TokenLogical
	case "NOT":
		tokenType = TokenLogical
	case "CONTAINS", "BEGINSWITH", "ENDSWITH", "LIKE", "IN", "BETWEEN":
		tokenType = TokenComparison
	}

	return Token{Type: tokenType, Value: tokenValue, Line: l.line, Column: startColumn}
}

func (l *Lexer) readNumber() Token {
	startColumn := l.column
	var value strings.Builder

	for l.position < len(l.input) && (unicode.IsDigit(rune(l.input[l.position])) || l.input[l.position] == '.') {
		value.WriteByte(l.input[l.position])
		l.advance()
	}

	return Token{Type: TokenNumber, Value: value.String(), Line: l.line, Column: startColumn}
}

func (l *Lexer) readComparison() Token {
	startColumn := l.column
	var value strings.Builder

	value.WriteByte(l.input[l.position])
	l.advance()

	// Handle two-character operators
	if l.position < len(l.input) {
		switch l.input[l.position-1] {
		case '=':
			if l.input[l.position] == '=' {
				value.WriteByte(l.input[l.position])
				l.advance()
			}
		case '!':
			if l.input[l.position] == '=' {
				value.WriteByte(l.input[l.position])
				l.advance()
			}
		case '<':
			if l.input[l.position] == '=' {
				value.WriteByte(l.input[l.position])
				l.advance()
			}
		case '>':
			if l.input[l.position] == '=' {
				value.WriteByte(l.input[l.position])
				l.advance()
			}
		}
	}

	return Token{Type: TokenComparison, Value: value.String(), Line: l.line, Column: startColumn}
}
