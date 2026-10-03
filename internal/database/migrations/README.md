# Database migrations

Forward-only PostgreSQL migrations live in this directory and are embedded into
the administrative binary. Migration filenames use `NNNN_description.sql`, where
the version is positive and the description contains lowercase ASCII letters,
digits, underscores, or hyphens.

Never edit a migration after it has shipped. Add a higher-numbered migration to
repair or evolve the schema. Applied file contents are protected by a SHA-256
checksum in `schema_migrations`; rollback is performed by restoring a tested
backup or by deploying a forward repair migration.

Issue #4 owns the initial schema, so this directory intentionally contains no SQL
migrations yet.
