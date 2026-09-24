package errinfo

import (
	"errors"
	"fmt"
	"testing"
)

// reason 会成为指标 label；格式失控就是基数失控，所以必须在声明时（进程启动）就失败。
func TestNewRejectsUnboundedReason(t *testing.T) {
	for _, bad := range []string{"", "cart_empty", "CART EMPTY", "USER_42a", "X"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%q) must panic", bad)
				}
			}()
			New(bad, "msg")
		}()
	}
}

// 最靠近故障的位置最有用：外层再次 Here 不能覆盖内层记录的抛错点。
func TestHereKeepsInnermostOrigin(t *testing.T) {
	inner := Here(errors.New("boom"))
	innerOrigin, _ := OriginOf(inner)
	outer := Here(fmt.Errorf("wrap: %w", inner))
	outerOrigin, ok := OriginOf(outer)
	if !ok || outerOrigin != innerOrigin {
		t.Fatalf("outer Here overwrote origin: inner=%v outer=%v", innerOrigin, outerOrigin)
	}
	if Here(nil) != nil {
		t.Fatal("Here(nil) must be nil")
	}
}
