package query

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/turdb/tur/pkg/keys"
)

// PropertyExpression represents a property access
type PropertyExpression struct {
	Name   string
	ColKey keys.ColKey
}

func (pe *PropertyExpression) Accept(visitor ExpressionVisitor) interface{} {
	return visitor.VisitProperty(pe)
}

func (pe *PropertyExpression) String() string {
	return pe.Name
}

// LiteralExpression represents a literal value
type LiteralExpression struct {
	Value interface{}
}

func (le *LiteralExpression) Accept(visitor ExpressionVisitor) interface{} {
	return visitor.VisitLiteral(le)
}

func (le *LiteralExpression) String() string {
	if str, ok := le.Value.(string); ok {
		return fmt.Sprintf("'%s'", str)
	}
	return fmt.Sprintf("%v", le.Value)
}

// ExpressionVisitor defines the visitor pattern for query expressions
type ExpressionVisitor interface {
	VisitComparison(expr *ComparisonExpression) interface{}
	VisitLogical(expr *LogicalExpression) interface{}
	VisitProperty(expr *PropertyExpression) interface{}
	VisitLiteral(expr *LiteralExpression) interface{}
}

// Parser parses query expressions
type Parser struct {
	lexer        *Lexer
	currentToken Token
}

// NewParser creates a new parser
func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	parser := &Parser{lexer: lexer}
	parser.advance() // Load first token
	return parser
}

func (p *Parser) advance() {
	p.currentToken = p.lexer.NextToken()
}

// Parse parses the query string into an expression tree
func (p *Parser) Parse() (QueryExpression, error) {
	if p.currentToken.Type == TokenEOF {
		return nil, fmt.Errorf("empty query")
	}
	return p.parseLogicalOr()
}

func (p *Parser) parseLogicalOr() (QueryExpression, error) {
	left, err := p.parseLogicalAnd()
	if err != nil {
		return nil, err
	}

	for p.currentToken.Type == TokenLogical && strings.ToUpper(p.currentToken.Value) == "OR" {
		p.advance()
		right, err := p.parseLogicalAnd()
		if err != nil {
			return nil, err
		}
		left = &LogicalExpression{Left: left, Operator: OpOr, Right: right}
	}

	return left, nil
}

func (p *Parser) parseLogicalAnd() (QueryExpression, error) {
	left, err := p.parseLogicalNot()
	if err != nil {
		return nil, err
	}

	for p.currentToken.Type == TokenLogical && strings.ToUpper(p.currentToken.Value) == "AND" {
		p.advance()
		right, err := p.parseLogicalNot()
		if err != nil {
			return nil, err
		}
		left = &LogicalExpression{Left: left, Operator: OpAnd, Right: right}
	}

	return left, nil
}

func (p *Parser) parseLogicalNot() (QueryExpression, error) {
	if p.currentToken.Type == TokenLogical && strings.ToUpper(p.currentToken.Value) == "NOT" {
		p.advance()
		expr, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		return &LogicalExpression{Operator: OpNot, Right: expr}, nil
	}

	return p.parseComparison()
}

func (p *Parser) parseComparison() (QueryExpression, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	if p.currentToken.Type == TokenComparison {
		op, err := p.parseComparisonOperator()
		if err != nil {
			return nil, err
		}
		p.advance()

		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}

		return &ComparisonExpression{Left: left, Operator: op, Right: right}, nil
	}

	return left, nil
}

func (p *Parser) parseComparisonOperator() (ComparisonOp, error) {
	switch p.currentToken.Value {
	case "==", "=":
		return OpEqual, nil
	case "!=":
		return OpNotEqual, nil
	case "<":
		return OpLess, nil
	case "<=":
		return OpLessEqual, nil
	case ">":
		return OpGreater, nil
	case ">=":
		return OpGreaterEqual, nil
	default:
		switch strings.ToUpper(p.currentToken.Value) {
		case "CONTAINS":
			return OpContains, nil
		case "BEGINSWITH":
			return OpBeginsWith, nil
		case "ENDSWITH":
			return OpEndsWith, nil
		case "LIKE":
			return OpLike, nil
		case "IN":
			return OpIn, nil
		case "BETWEEN":
			return OpBetween, nil
		default:
			return OpEqual, fmt.Errorf("unknown comparison operator: %s", p.currentToken.Value)
		}
	}
}

func (p *Parser) parsePrimary() (QueryExpression, error) {
	switch p.currentToken.Type {
	case TokenLeftParen:
		p.advance()
		expr, err := p.parseLogicalOr()
		if err != nil {
			return nil, err
		}
		if p.currentToken.Type != TokenRightParen {
			return nil, fmt.Errorf("expected ')', got %s", p.currentToken.Value)
		}
		p.advance()
		return expr, nil

	case TokenIdentifier:
		name := p.currentToken.Value
		p.advance()
		return &PropertyExpression{Name: name}, nil

	case TokenString:
		value := p.currentToken.Value
		p.advance()
		return &LiteralExpression{Value: value}, nil

	case TokenNumber:
		valueStr := p.currentToken.Value
		p.advance()

		if strings.Contains(valueStr, ".") {
			if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
				return &LiteralExpression{Value: value}, nil
			}
		} else {
			if value, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
				return &LiteralExpression{Value: value}, nil
			}
		}
		return nil, fmt.Errorf("invalid number: %s", valueStr)

	default:
		return nil, fmt.Errorf("unexpected token: %s", p.currentToken.Value)
	}
}
