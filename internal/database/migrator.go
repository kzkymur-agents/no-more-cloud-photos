// Package database provides PostgreSQL migration support.
package database

import (
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	migrationsDirectory = "migrations"
	migrationLockKey    = int64(0x4e4d43504d494752) // "NMCPMIGR"
)

var (
	// ErrUnknownAppliedVersion means the database contains a migration that is
	// not present in this binary.
	ErrUnknownAppliedVersion = errors.New("unknown applied migration version")
	// ErrChecksumMismatch means an embedded migration changed after it was applied.
	ErrChecksumMismatch = errors.New("migration checksum mismatch")
	// ErrInvalidMigrationHistory means applied migrations are not an exact prefix
	// of the embedded migration sequence.
	ErrInvalidMigrationHistory = errors.New("invalid migration history")
)

//go:embed migrations
var embeddedMigrations embed.FS

// Status describes whether the database schema is ready for this binary.
type Status struct {
	CurrentVersion  int64  `json:"current_version"`
	ExpectedVersion int64  `json:"expected_version"`
	Pending         bool   `json:"pending"`
	Drift           bool   `json:"drift"`
	DriftReason     string `json:"drift_reason,omitempty"`
}

// Ready reports whether there are no pending migrations or history drift.
func (s Status) Ready() bool {
	return !s.Pending && !s.Drift
}

// Migrator applies the immutable migrations embedded in the binary.
type Migrator struct {
	pool       *pgxpool.Pool
	migrations []migration
}

type migration struct {
	version  int64
	name     string
	checksum string
	sql      string
}

type appliedMigration struct {
	version  int64
	name     string
	checksum string
}

// NewMigrator discovers and validates the migrations embedded in this binary.
func NewMigrator(pool *pgxpool.Pool) (*Migrator, error) {
	if pool == nil {
		return nil, errors.New("database migration pool is nil")
	}

	migrations, err := discoverMigrations(embeddedMigrations, migrationsDirectory)
	if err != nil {
		return nil, fmt.Errorf("discover embedded migrations: %w", err)
	}
	return newMigrator(pool, migrations), nil
}

func newMigrator(pool *pgxpool.Pool, migrations []migration) *Migrator {
	return &Migrator{pool: pool, migrations: slices.Clone(migrations)}
}

// Status inspects migration state without creating or modifying database objects.
func (m *Migrator) Status(ctx context.Context) (Status, error) {
	status := Status{ExpectedVersion: expectedVersion(m.migrations)}

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return status, fmt.Errorf("acquire connection for migration status: %w", err)
	}
	defer conn.Release()

	var historyExists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&historyExists); err != nil {
		return status, fmt.Errorf("check migration history table: %w", err)
	}
	if !historyExists {
		status.Pending = len(m.migrations) > 0
		return status, nil
	}

	applied, err := readApplied(ctx, conn)
	if err != nil {
		return status, err
	}
	status.CurrentVersion = currentVersion(applied)

	pending, err := validateHistory(m.migrations, applied)
	if err != nil {
		status.Drift = true
		status.DriftReason = err.Error()
		return status, nil
	}
	status.Pending = len(pending) > 0
	return status, nil
}

// Up creates migration history infrastructure and applies all pending migrations.
// The advisory lock, validation, and migrations share one transaction so concurrent
// callers serialize and partially applied migration sets are never visible.
func (m *Migrator) Up(ctx context.Context) error {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version bigint PRIMARY KEY CHECK (version > 0),
			name text NOT NULL CHECK (name <> ''),
			checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		return fmt.Errorf("create migration history table: %w", err)
	}

	applied, err := readApplied(ctx, tx)
	if err != nil {
		return err
	}
	pending, err := validateHistory(m.migrations, applied)
	if err != nil {
		return err
	}

	for _, migration := range pending {
		if _, err := tx.Exec(ctx, migration.sql); err != nil {
			return fmt.Errorf("apply migration %d_%s: %w", migration.version, migration.name, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO schema_migrations (version, name, checksum)
			VALUES ($1, $2, $3)`, migration.version, migration.name, migration.checksum); err != nil {
			return fmt.Errorf("record migration %d_%s: %w", migration.version, migration.name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func readApplied(ctx context.Context, q queryer) ([]appliedMigration, error) {
	rows, err := q.Query(ctx, `SELECT version, name, checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("read migration history: %w", err)
	}
	defer rows.Close()

	var applied []appliedMigration
	for rows.Next() {
		var row appliedMigration
		if err := rows.Scan(&row.version, &row.name, &row.checksum); err != nil {
			return nil, fmt.Errorf("read migration history row: %w", err)
		}
		applied = append(applied, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read migration history rows: %w", err)
	}
	return applied, nil
}

func validateHistory(migrations []migration, applied []appliedMigration) ([]migration, error) {
	byVersion := make(map[int64]migration, len(migrations))
	for _, migration := range migrations {
		byVersion[migration.version] = migration
	}

	for i, row := range applied {
		expected, known := byVersion[row.version]
		if !known {
			return nil, fmt.Errorf("%w: %d", ErrUnknownAppliedVersion, row.version)
		}
		if row.checksum != expected.checksum {
			return nil, fmt.Errorf("%w for version %d", ErrChecksumMismatch, row.version)
		}
		if row.name != expected.name {
			return nil, fmt.Errorf("%w: version %d name is %q, expected %q", ErrInvalidMigrationHistory, row.version, row.name, expected.name)
		}
		if i >= len(migrations) || row.version != migrations[i].version {
			return nil, fmt.Errorf("%w: version %d is not the next expected version", ErrInvalidMigrationHistory, row.version)
		}
	}

	return slices.Clone(migrations[len(applied):]), nil
}

func discoverMigrations(fsys fs.FS, directory string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, directory)
	if err != nil {
		return nil, err
	}

	var migrations []migration
	versions := make(map[int64]string)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version, name, err := parseMigrationFilename(entry.Name())
		if err != nil {
			return nil, err
		}
		if previous, exists := versions[version]; exists {
			return nil, fmt.Errorf("duplicate migration version %d in %q and %q", version, previous, entry.Name())
		}

		contents, err := fs.ReadFile(fsys, directory+"/"+entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		if len(contents) == 0 {
			return nil, fmt.Errorf("migration %q is empty", entry.Name())
		}
		versions[version] = entry.Name()
		migrations = append(migrations, migration{
			version:  version,
			name:     name,
			checksum: checksumSQL(contents),
			sql:      string(contents),
		})
	}

	slices.SortFunc(migrations, func(a, b migration) int {
		if a.version < b.version {
			return -1
		}
		if a.version > b.version {
			return 1
		}
		return 0
	})
	return migrations, nil
}

func parseMigrationFilename(filename string) (int64, string, error) {
	if !strings.HasSuffix(filename, ".sql") {
		return 0, "", fmt.Errorf("invalid migration filename %q", filename)
	}
	stem := strings.TrimSuffix(filename, ".sql")
	versionText, name, found := strings.Cut(stem, "_")
	if !found || versionText == "" || name == "" || strings.Trim(name, "abcdefghijklmnopqrstuvwxyz0123456789_-") != "" {
		return 0, "", fmt.Errorf("invalid migration filename %q", filename)
	}
	if name[0] < 'a' || name[0] > 'z' {
		return 0, "", fmt.Errorf("invalid migration filename %q", filename)
	}
	for _, digit := range versionText {
		if digit < '0' || digit > '9' {
			return 0, "", fmt.Errorf("invalid migration filename %q", filename)
		}
	}
	version, err := strconv.ParseInt(versionText, 10, 64)
	if err != nil || version <= 0 {
		return 0, "", fmt.Errorf("invalid migration version in %q", filename)
	}
	return version, name, nil
}

func checksumSQL(sql []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(sql))
}

func expectedVersion(migrations []migration) int64 {
	if len(migrations) == 0 {
		return 0
	}
	return migrations[len(migrations)-1].version
}

func currentVersion(applied []appliedMigration) int64 {
	if len(applied) == 0 {
		return 0
	}
	return applied[len(applied)-1].version
}
