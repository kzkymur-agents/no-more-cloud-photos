package database

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigratorIntegration(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	t.Run("status is read-only and Up initializes history", func(t *testing.T) {
		pool := integrationPool(t, databaseURL)
		migrator, err := NewMigrator(pool)
		if err != nil {
			t.Fatalf("NewMigrator() error = %v", err)
		}

		status, err := migrator.Status(context.Background())
		if err != nil {
			t.Fatalf("Status() error = %v", err)
		}
		if status.CurrentVersion != 0 || status.ExpectedVersion != 0 || !status.Ready() {
			t.Fatalf("Status() = %+v, want ready version zero", status)
		}
		var historyExists bool
		if err := pool.QueryRow(context.Background(), `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&historyExists); err != nil {
			t.Fatalf("check history table after Status: %v", err)
		}
		if historyExists {
			t.Fatal("Status() created schema_migrations")
		}

		if err := migrator.Up(context.Background()); err != nil {
			t.Fatalf("Up() error = %v", err)
		}
		if err := pool.QueryRow(context.Background(), `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&historyExists); err != nil {
			t.Fatalf("check history table after Up: %v", err)
		}
		if !historyExists {
			t.Fatal("Up() did not create schema_migrations")
		}
	})

	t.Run("concurrent Up calls serialize", func(t *testing.T) {
		pool := integrationPool(t, databaseURL)
		migrationEntry := testMigration(1, "serialized", "SELECT pg_sleep(0.2)")
		migrator := newMigrator(pool, []migration{migrationEntry})

		start := make(chan struct{})
		errorsByCall := make(chan error, 2)
		var ready sync.WaitGroup
		ready.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				ready.Done()
				<-start
				errorsByCall <- migrator.Up(context.Background())
			}()
		}
		ready.Wait()
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-errorsByCall; err != nil {
				t.Fatalf("concurrent Up() error = %v", err)
			}
		}

		var count int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
			t.Fatalf("count migration history: %v", err)
		}
		if count != 1 {
			t.Fatalf("migration history count = %d, want 1", count)
		}
	})

	t.Run("unknown applied version is drift", func(t *testing.T) {
		pool := integrationPool(t, databaseURL)
		migrator, err := NewMigrator(pool)
		if err != nil {
			t.Fatalf("NewMigrator() error = %v", err)
		}
		if err := migrator.Up(context.Background()); err != nil {
			t.Fatalf("Up() initialization error = %v", err)
		}
		if _, err := pool.Exec(context.Background(), `
			INSERT INTO schema_migrations (version, name, checksum)
			VALUES (99, 'future', $1)`, strings.Repeat("0", 64)); err != nil {
			t.Fatalf("insert unknown history row: %v", err)
		}

		status, err := migrator.Status(context.Background())
		if err != nil {
			t.Fatalf("Status() error = %v", err)
		}
		if !status.Drift || status.Ready() {
			t.Fatalf("Status() = %+v, want drift and not ready", status)
		}
		if err := migrator.Up(context.Background()); !errors.Is(err, ErrUnknownAppliedVersion) {
			t.Fatalf("Up() error = %v, want ErrUnknownAppliedVersion", err)
		}
	})

	t.Run("checksum change is drift", func(t *testing.T) {
		pool := integrationPool(t, databaseURL)
		migrationEntry := testMigration(1, "checksum", "SELECT 1")
		migrator := newMigrator(pool, []migration{migrationEntry})
		if err := migrator.Up(context.Background()); err != nil {
			t.Fatalf("Up() error = %v", err)
		}
		if _, err := pool.Exec(context.Background(), `
			UPDATE schema_migrations SET checksum = $1 WHERE version = 1`, strings.Repeat("0", 64)); err != nil {
			t.Fatalf("change stored checksum: %v", err)
		}

		status, err := migrator.Status(context.Background())
		if err != nil {
			t.Fatalf("Status() error = %v", err)
		}
		if !status.Drift || status.Ready() {
			t.Fatalf("Status() = %+v, want drift and not ready", status)
		}
		if err := migrator.Up(context.Background()); !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("Up() error = %v, want ErrChecksumMismatch", err)
		}
	})
}

func testMigration(version int64, name, sql string) migration {
	return migration{version: version, name: name, checksum: checksumSQL([]byte(sql)), sql: sql}
}

func integrationPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to integration database: %v", err)
	}
	t.Cleanup(admin.Close)

	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("generate test schema name: %v", err)
	}
	schema := "nmcp_migration_test_" + hex.EncodeToString(random)
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = identifier
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect using isolated test schema: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping integration database: %v", err)
	}
	return pool
}
