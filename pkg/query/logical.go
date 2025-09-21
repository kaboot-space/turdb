package query

import (
	"fmt"
)

// TokenType represents different types of tokens in query expressions
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdentifier
	TokenString
	TokenNumber
	TokenOperator
	TokenComparison
	TokenLogical
	TokenLeftParen
	TokenRightParen
	TokenComma
	TokenDot
)

// Token represents a single token in the query
type Token struct {
	Type   TokenType
	Value  string
	Line   int
	Column int
}

// ComparisonOp represents comparison operators
type ComparisonOp int

const (
	OpEqual ComparisonOp = iota
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
	OpContains
	OpBeginsWith
	OpEndsWith
	OpLike
	OpIn
	OpBetween
)

// LogicalOp represents logical operators
type LogicalOp int

const (
	OpAnd LogicalOp = iota
	OpOr
	OpNot
)

// LogicalExpression represents a logical operation
type LogicalExpression struct {
	Left     QueryExpression
	Operator LogicalOp
	Right    QueryExpression
}

func (le *LogicalExpression) Accept(visitor ExpressionVisitor) interface{} {
	return visitor.VisitLogical(le)
}

func (le *LogicalExpression) String() string {
	if le.Operator == OpNot {
		return fmt.Sprintf("NOT %s", le.Right.String())
	}
	return fmt.Sprintf("(%s %s %s)", le.Left.String(), le.operatorString(), le.Right.String())
}

func (le *LogicalExpression) operatorString() string {
	switch le.Operator {
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpNot:
		return "NOT"
	default:
		return "UNKNOWN"
	}
}
