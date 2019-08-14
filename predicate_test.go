package binq

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPredicateToMatcher(t *testing.T) {
	t.Parallel()
	predicates := loadPredicates(t, "predicates.pbascii")

	type predMatch struct {
		name          string
		binary        []byte
		expectedMatch bool
		expectedErr   bool
	}
	type testCase struct {
		predicateIndex predicateIndex
		tests          []predMatch
	}
	cases := []testCase{
		{predU64le0_eq_su64100, []predMatch{
			{"100", makeBytes(t, u64le(100)), true, false},
			{"101", makeBytes(t, u64le(101)), false, false},
			{"missing-bytes", makeBytes(t), false, true},
		}},
		{u32le0_lt_u84, []predMatch{
			{"5_100", makeBytes(t, u32le(5), u8(100)), true, false},
			{"100_100", makeBytes(t, u32le(100), u8(100)), false, false},
			{"100_5", makeBytes(t, u32le(100), u8(5)), false, false},
		}},
		{u64le0_lte_u16be8, []predMatch{
			{"5_100", makeBytes(t, u64le(5), u16be(100)), true, false},
			{"100_100", makeBytes(t, u64le(100), u16be(100)), true, false},
			{"100_5", makeBytes(t, u64le(100), u16be(5)), false, false},
		}},
		{u64le0_gt_u16be8, []predMatch{
			{"5_100", makeBytes(t, u64le(5), u16be(100)), false, false},
			{"100_100", makeBytes(t, u64le(100), u16be(100)), false, false},
			{"100_5", makeBytes(t, u64le(100), u16be(5)), true, false},
		}},
		{u64le0_gte_u16be8, []predMatch{
			{"5_100", makeBytes(t, u64le(5), u16be(100)), false, false},
			{"100_100", makeBytes(t, u64le(100), u16be(100)), true, false},
			{"100_5", makeBytes(t, u64le(100), u16be(5)), true, false},
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.predicateIndex.String(), func(t *testing.T) {
			t.Parallel()
			predicate := predicates[tc.predicateIndex]
			matcher, err := PredicateToMatcher(predicate)
			if err != nil {
				t.Fatal(err)
			}
			for _, pc := range tc.tests {
				pc := pc
				t.Run(pc.name, func(t *testing.T) {
					t.Parallel()
					matches, err := matcher.Match(pc.binary)
					assert.Equal(t, pc.expectedErr, err != nil, "(un)expected error: %s", err)
					assert.Equal(t, pc.expectedMatch, matches, "(un)expected match")
				})
			}
		})
	}
}

func TestPredicateToMatch_BooleanBinaryOperationsOnUintTypes(t *testing.T) {
	t.Parallel()
	for _, aType := range uintValueTypes {
		aType := aType
		t.Run(aType.String(), func(t *testing.T) {
			t.Parallel()
			for _, bType := range uintValueTypes {
				bType := bType
				t.Run(bType.String(), func(t *testing.T) {
					t.Parallel()
					for op := range booleanOps {
						op := op
						t.Run(op.String(), func(t *testing.T) {
							t.Parallel()
							aBytes := makeBytes(t, makeValueTypeValue(t, aType))
							bBytes := makeBytes(t, makeValueTypeValue(t, bType))
							bytes := makeBytes(t, aBytes, bBytes)
							predicate := &Predicate{
								Predicate: &Predicate_Expression{
									Expression: &Expression{
										Expression: &Expression_BinaryOperation{
											BinaryOperation: &BinaryOperation{
												Left:         makeValueExpression(aType, 0),
												Right:        makeValueExpression(bType, uint64(len(aBytes))),
												BinaryOpCode: op,
											},
										},
									},
								},
							}

							matcher, err := PredicateToMatcher(predicate)
							if !assert.NoError(t, err) {
								return
							}
							_, err = matcher.Match(bytes)
							assert.NoError(t, err)
						})
					}
				})
			}
		})
	}
}

func TestPredicateToMatch_BooleanBinaryOperationsOnScalarTypes(t *testing.T) {
	scalars := []struct {
		name       string
		expression *Expression
	}{
		{"bool", makeScalarExpression(t, false)},
		{"u32", makeScalarExpression(t, uint32(0))},
		{"u64", makeScalarExpression(t, uint64(0))},
	}
	t.Parallel()
	for _, aScalar := range scalars {
		aScalar := aScalar
		t.Run(aScalar.name, func(t *testing.T) {
			t.Parallel()
			for _, bScalar := range scalars {
				bScalar := bScalar
				t.Run(bScalar.name, func(t *testing.T) {
					t.Parallel()
					for op := range booleanOps {
						op := op
						t.Run(op.String(), func(t *testing.T) {
							t.Parallel()
							predicate := &Predicate{
								Predicate: &Predicate_Expression{
									Expression: &Expression{
										Expression: &Expression_BinaryOperation{
											BinaryOperation: &BinaryOperation{
												Left:         aScalar.expression,
												Right:        bScalar.expression,
												BinaryOpCode: op,
											},
										},
									},
								},
							}
							matcher, err := PredicateToMatcher(predicate)
							if !assert.NoError(t, err) {
								return
							}
							_, err = matcher.Match([]byte{})
							assert.NoError(t, err)
						})
					}
				})
			}
		})
	}
}

func TestPredicateToMatch_Any(t *testing.T) {
	t.Parallel()
	predicate := &Predicate{
		Predicate: &Predicate_Any{
			Any: &Expressions{
				Expressions: []*Expression{
					makeScalarExpression(t, true),
				},
			},
		},
	}
	matcher, err := PredicateToMatcher(predicate)
	if !assert.NoError(t, err) {
		return
	}
	result, err := matcher.Match([]byte{})
	assert.NoError(t, err)
	assert.True(t, result)
}

func TestPredicateToMatch_All(t *testing.T) {
	t.Parallel()
	predicate := &Predicate{
		Predicate: &Predicate_All{
			All: &Expressions{
				Expressions: []*Expression{
					makeScalarExpression(t, true),
				},
			},
		},
	}
	matcher, err := PredicateToMatcher(predicate)
	if !assert.NoError(t, err) {
		return
	}
	result, err := matcher.Match([]byte{})
	assert.NoError(t, err)
	assert.True(t, result)
}

func TestPredicateToMatch_NonBoolean(t *testing.T) {
	t.Parallel()
	predicate := &Predicate{
		Predicate: &Predicate_All{
			All: &Expressions{
				Expressions: []*Expression{
					makeScalarExpression(t, uint64(1)),
				},
			},
		},
	}
	_, err := PredicateToMatcher(predicate)
	assert.Error(t, err)
}
