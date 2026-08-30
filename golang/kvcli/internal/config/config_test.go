package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadUsesExplicitConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kvcli.yaml")
	if err := os.WriteFile(path, []byte("server:\n  host: 192.0.2.10\n  port: 7443\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Host != "192.0.2.10" || cfg.Server.Port != 7443 {
		t.Fatalf("unexpected server configuration: %+v", cfg.Server)
	}
}

func TestLoadFallsBackToSafeLocalDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Host != "localhost" || cfg.Server.Port != 7000 {
		t.Fatalf("unexpected defaults: %+v", cfg.Server)
	}
}
