package database

import (
	"errors"
	"testing"
	"testing/fstest"
)

func TestParseMigrationFilename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filename string
		version  int64
		name     string
		wantErr  bool
	}{
		{filename: "0001_initial_schema.sql", version: 1, name: "initial_schema"},
		{filename: "42_add-index.sql", version: 42, name: "add-index"},
		{filename: "0_zero.sql", wantErr: true},
		{filename: "one_name.sql", wantErr: true},
		{filename: "1_Name.sql", wantErr: true},
		{filename: "1_.sql", wantErr: true},
		{filename: "1_name.txt", wantErr: true},
		{filename: "name.sql", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.filename, func(t *testing.T) {
			version, name, err := parseMigrationFilename(test.filename)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMigrationFilename() error = %v", err)
			}
			if version != test.version || name != test.name {
				t.Fatalf("parseMigrationFilename() = (%d, %q), want (%d, %q)", version, name, test.version, test.name)
			}
		})
	}
}

func TestChecksumSQL(t *testing.T) {
	t.Parallel()

	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got := checksumSQL([]byte("abc")); got != want {
		t.Fatalf("checksumSQL() = %q, want %q", got, want)
	}
}

func TestDiscoverMigrations(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"migrations/README.md":          {Data: []byte("documentation")},
		"migrations/0010_add_index.sql": {Data: []byte("CREATE INDEX example;")},
		"migrations/0002_create.sql":    {Data: []byte("CREATE TABLE example();")},
	}
	migrations, err := discoverMigrations(fsys, "migrations")
	if err != nil {
		t.Fatalf("discoverMigrations() error = %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("len(migrations) = %d, want 2", len(migrations))
	}
	if migrations[0].version != 2 || migrations[1].version != 10 {
		t.Fatalf("migration versions = [%d, %d], want [2, 10]", migrations[0].version, migrations[1].version)
	}
	if migrations[0].checksum != checksumSQL(fsys["migrations/0002_create.sql"].Data) {
		t.Fatal("discovered checksum does not match migration contents")
	}

	fsys["migrations/0002_create.sql"].Data[0] = 'X'
	if migrations[0].sql != "CREATE TABLE example();" {
		t.Fatal("discovered migration changed after its source filesystem was mutated")
	}
}

func TestDiscoverMigrationsRejectsDuplicateVersions(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"migrations/1_first.sql":   {Data: []byte("SELECT 1")},
		"migrations/01_second.sql": {Data: []byte("SELECT 2")},
	}
	if _, err := discoverMigrations(fsys, "migrations"); err == nil {
		t.Fatal("discoverMigrations() expected duplicate version error")
	}
}

func TestValidateHistory(t *testing.T) {
	t.Parallel()

	migrations := []migration{
		{version: 1, name: "first", checksum: "checksum-1", sql: "SELECT 1"},
		{version: 3, name: "third", checksum: "checksum-3", sql: "SELECT 3"},
	}
	tests := []struct {
		name        string
		applied     []appliedMigration
		wantPending int
		wantErr     error
	}{
		{name: "empty", wantPending: 2},
		{
			name:        "valid prefix",
			applied:     []appliedMigration{{version: 1, name: "first", checksum: "checksum-1"}},
			wantPending: 1,
		},
		{
			name: "complete",
			applied: []appliedMigration{
				{version: 1, name: "first", checksum: "checksum-1"},
				{version: 3, name: "third", checksum: "checksum-3"},
			},
		},
		{
			name:    "unknown version",
			applied: []appliedMigration{{version: 2, name: "second", checksum: "checksum-2"}},
			wantErr: ErrUnknownAppliedVersion,
		},
		{
			name:    "checksum drift",
			applied: []appliedMigration{{version: 1, name: "first", checksum: "changed"}},
			wantErr: ErrChecksumMismatch,
		},
		{
			name:    "renamed migration",
			applied: []appliedMigration{{version: 1, name: "renamed", checksum: "checksum-1"}},
			wantErr: ErrInvalidMigrationHistory,
		},
		{
			name:    "non-prefix downgrade shape",
			applied: []appliedMigration{{version: 3, name: "third", checksum: "checksum-3"}},
			wantErr: ErrInvalidMigrationHistory,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pending, err := validateHistory(migrations, test.applied)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("validateHistory() error = %v, want %v", err, test.wantErr)
			}
			if err == nil && len(pending) != test.wantPending {
				t.Fatalf("len(pending) = %d, want %d", len(pending), test.wantPending)
			}
		})
	}
}
