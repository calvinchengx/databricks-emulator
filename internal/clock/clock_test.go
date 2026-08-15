package clock

import "testing"

func TestNowAdvancesAndFreezes(t *testing.T) {
	c := New()
	c.realNow = func() int64 { return 1_000 }
	if c.Now() != 1_000 {
		t.Fatalf("Now = %d", c.Now())
	}
	c.Advance(30)
	if c.Now() != 1_030 {
		t.Fatalf("after advance Now = %d", c.Now())
	}
	c.Freeze()
	c.realNow = func() int64 { return 2_000 }
	if c.Now() != 1_030 {
		t.Fatalf("frozen Now = %d", c.Now())
	}
	c.Advance(5)
	if c.Now() != 1_035 {
		t.Fatalf("frozen advance Now = %d", c.Now())
	}
	c.Unfreeze()
	if c.Now() != 1_035 {
		t.Fatalf("unfrozen Now = %d", c.Now())
	}
	c.Freeze()
	c.Freeze() // idempotent
	c.Unfreeze()
	c.Unfreeze()
}
