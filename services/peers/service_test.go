package peers

import (
	"testing"

	"peerdb-playground/gen"
)

func TestValidatePeerTypeMatchesConfig(t *testing.T) {
	testCases := []struct {
		name    string
		peer    *gen.Peer
		wantErr bool
	}{
		{
			name: "postgres matches",
			peer: &gen.Peer{
				Type: gen.PeerType_POSTGRES,
				Config: &gen.Peer_PostgresConfig{
					PostgresConfig: &gen.PostgresConfig{},
				},
			},
		},
		{
			name: "mysql matches",
			peer: &gen.Peer{
				Type: gen.PeerType_MYSQL,
				Config: &gen.Peer_MysqlConfig{
					MysqlConfig: &gen.MysqlConfig{},
				},
			},
		},
		{
			name: "mismatch returns error",
			peer: &gen.Peer{
				Type: gen.PeerType_MYSQL,
				Config: &gen.Peer_PostgresConfig{
					PostgresConfig: &gen.PostgresConfig{},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePeerTypeMatchesConfig(tc.peer)
			if tc.wantErr && err == nil {
				t.Fatal("expected validatePeerTypeMatchesConfig to return an error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validatePeerTypeMatchesConfig returned error: %v", err)
			}
		})
	}
}
