package binq

import (
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"os"
)

const (
	predU64le0_eq_su64100 predicateIndex = iota
	u32le0_lt_u84
	u64le0_lte_u16be8
	u64le0_gt_u16be8
	u64le0_gte_u16be8
)

type predicateIndex uint8

func (p predicateIndex) String() string {
	switch p {
	case predU64le0_eq_su64100:
		return "predU64le0_eq_su64100"
	case u32le0_lt_u84:
		return "u32le0_lt_u84"
	case u64le0_lte_u16be8:
		return "u64le0_lte_u16be8"
	case u64le0_gt_u16be8:
		return "u64le0_gt_u16be8"
	case u64le0_gte_u16be8:
		return "u64le0_gte_u16be8"
	default:
		return "<unknown predicate>"
	}
}

func loadPredicates(t TestType, name string) []*Predicate {
	t.Helper()
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	pb := &Predicates{}
	if err := proto.UnmarshalText(string(b), pb); err != nil {
		t.Fatal(err)
	}
	return pb.Predicates
}
