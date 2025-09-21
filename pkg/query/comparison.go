package query

import (
	"fmt"
)

// QueryExpression represents a parsed query expression
type QueryExpression interface {
	Accept(visitor ExpressionVisitor) interface{}
	String() string
}

// ComparisonExpression represents a comparison operation
type ComparisonExpression struct {
	Left     QueryExpression
	Operator ComparisonOp
	Right    QueryExpression
}

func (ce *ComparisonExpression) Accept(visitor ExpressionVisitor) interface{} {
	return visitor.VisitComparison(ce)
}

func (ce *ComparisonExpression) String() string {
	return fmt.Sprintf("(%s %s %s)", ce.Left.String(), ce.operatorString(), ce.Right.String())
}

func (ce *ComparisonExpression) operatorString() string {
	switch ce.Operator {
	case OpEqual:
		return "=="
	case OpNotEqual:
		return "!="
	case OpLess:
		return "<"
	case OpLessEqual:
		return "<="
	case OpGreater:
		return ">"
	case OpGreaterEqual:
		return ">="
	case OpContains:
		return "CONTAINS"
	case OpBeginsWith:
		return "BEGINSWITH"
	case OpEndsWith:
		return "ENDSWITH"
	case OpLike:
		return "LIKE"
	case OpIn:
		return "IN"
	case OpBetween:
		return "BETWEEN"
	default:
		return "UNKNOWN"
	}
}
