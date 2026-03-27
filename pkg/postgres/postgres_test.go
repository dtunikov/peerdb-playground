package postgres

import "testing"

func TestParseReplicationSlotLSN(t *testing.T) {
	t.Run("prefers confirmed flush lsn", func(t *testing.T) {
		confirmed := "0/16B6C50"
		restart := "0/16B6C40"

		lsn, err := ParseReplicationSlotLSN(&confirmed, &restart)
		if err != nil {
			t.Fatalf("ParseReplicationSlotLSN returned error: %v", err)
		}
		if got, want := lsn.String(), confirmed; got != want {
			t.Fatalf("unexpected lsn: got %s want %s", got, want)
		}
	})

	t.Run("falls back to restart lsn", func(t *testing.T) {
		restart := "0/16B6C40"

		lsn, err := ParseReplicationSlotLSN(nil, &restart)
		if err != nil {
			t.Fatalf("ParseReplicationSlotLSN returned error: %v", err)
		}
		if got, want := lsn.String(), restart; got != want {
			t.Fatalf("unexpected lsn: got %s want %s", got, want)
		}
	})

	t.Run("returns zero when both are absent", func(t *testing.T) {
		lsn, err := ParseReplicationSlotLSN(nil, nil)
		if err != nil {
			t.Fatalf("ParseReplicationSlotLSN returned error: %v", err)
		}
		if lsn != 0 {
			t.Fatalf("expected zero lsn, got %s", lsn.String())
		}
	})
}
