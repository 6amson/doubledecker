-- Drop legacy tables from query builder era
DROP TABLE IF EXISTS saved_queries CASCADE;
DROP TABLE IF EXISTS uploads CASCADE;

-- Clean up and enhance users table with name and user_type persona
ALTER TABLE users 
    DROP COLUMN IF EXISTS total_queries,
    DROP COLUMN IF EXISTS total_files_processed,
    DROP COLUMN IF EXISTS total_saved_queries;

ALTER TABLE users 
    ADD COLUMN IF NOT EXISTS name VARCHAR(255) NOT NULL DEFAULT 'User',
    ADD COLUMN IF NOT EXISTS user_type VARCHAR(50) NOT NULL DEFAULT 'ARTIST';

DO $$ 
BEGIN
    ALTER TABLE users ADD CONSTRAINT valid_user_type CHECK (user_type IN ('ARTIST', 'MANAGER', 'LABEL', 'PUBLISHER'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Create workspaces table (Label, Manager, or Independent Artist catalog account)
CREATE TABLE IF NOT EXISTS workspaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    name VARCHAR(255) NOT NULL,
    storage_used_bytes BIGINT DEFAULT 0 NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workspaces_owner_user_id ON workspaces(owner_user_id);

-- Catalog Metadata: Artists (Profile-Scoped to owner_user_id)
CREATE TABLE IF NOT EXISTS artists (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE(owner_user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_artists_owner_user_id ON artists(owner_user_id);

-- Catalog Metadata: Albums / Releases (Profile-Scoped to owner_user_id)
CREATE TABLE IF NOT EXISTS albums (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    artist_id UUID NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    upc VARCHAR(100),
    release_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE(owner_user_id, upc)
);
CREATE INDEX IF NOT EXISTS idx_albums_owner_user_id ON albums(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_albums_artist_id ON albums(artist_id);

-- Catalog Metadata: Tracks / Recordings (Profile-Scoped to owner_user_id)
CREATE TABLE IF NOT EXISTS tracks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    artist_id UUID NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    album_id UUID REFERENCES albums(id) ON DELETE SET NULL,
    isrc VARCHAR(100) NOT NULL,
    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE(owner_user_id, isrc)
);
CREATE INDEX IF NOT EXISTS idx_tracks_owner_user_id ON tracks(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_tracks_artist_id ON tracks(artist_id);
CREATE INDEX IF NOT EXISTS idx_tracks_album_id ON tracks(album_id);

-- Payee Contact Book (Profile-Scoped to owner_user_id)
CREATE TABLE IF NOT EXISTS payees (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    bank_account VARCHAR(255),
    tax_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE(owner_user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_payees_owner_user_id ON payees(owner_user_id);

-- Cascading Splits with Temporal Bounds & Explicit Foreign Keys
CREATE TABLE IF NOT EXISTS cascading_splits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    album_id UUID REFERENCES albums(id) ON DELETE CASCADE,
    track_id UUID REFERENCES tracks(id) ON DELETE CASCADE,
    payee_id UUID REFERENCES payees(id) ON DELETE RESTRICT,
    payee_name VARCHAR(255) NOT NULL,
    percentage NUMERIC(5, 2) NOT NULL,
    effective_from DATE,
    effective_to DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT one_target_only CHECK (
        (artist_id IS NOT NULL)::int + 
        (album_id IS NOT NULL)::int + 
        (track_id IS NOT NULL)::int <= 1
    ),
    CONSTRAINT valid_percentage CHECK (percentage >= 0.01 AND percentage <= 100.00),
    CONSTRAINT valid_date_range CHECK (effective_to IS NULL OR effective_from <= effective_to)
);
CREATE INDEX IF NOT EXISTS idx_cascading_splits_lookup ON cascading_splits (workspace_id, track_id, album_id, artist_id, effective_from, effective_to);

-- Datasets (Normalized Parquet Files in S3)
CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    distributor_source VARCHAR(100) NOT NULL,
    filename VARCHAR(255) NOT NULL DEFAULT 'untitled.csv',
    s3_parquet_key VARCHAR(512) NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    row_count BIGINT DEFAULT 0 NOT NULL,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_datasets_workspace_id ON datasets(workspace_id);
CREATE INDEX IF NOT EXISTS idx_datasets_status ON datasets(status);

-- Exchange Rates (Daily currency conversion)
CREATE TABLE IF NOT EXISTS fx_rates (
    date DATE NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    rate_to_usd NUMERIC(10, 6) NOT NULL,
    PRIMARY KEY (date, currency_code)
);

-- Postgres-Native Job Queue Table (for Path B background ingestion)
CREATE TABLE IF NOT EXISTS background_jobs (
    id BIGSERIAL PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    s3_staging_key VARCHAR(512) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER DEFAULT 0 NOT NULL,
    max_attempts INTEGER DEFAULT 3 NOT NULL,
    run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    error_log TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_background_jobs_poll ON background_jobs (status, run_at) WHERE status = 'PENDING';
