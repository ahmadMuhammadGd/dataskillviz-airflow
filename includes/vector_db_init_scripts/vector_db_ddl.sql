DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        CREATE EXTENSION vector;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'warehouse') THEN
        CREATE SCHEMA warehouse;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'staging') THEN
        CREATE SCHEMA staging;
    END IF;
END
$$;


CREATE TABLE IF NOT EXISTS warehouse.tags_dim (
    tag_id BIGSERIAL PRIMARY KEY,
    tag VARCHAR(32) UNIQUE NOT NULL,
    embedding VECTOR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.titles (
    title_id SMALLSERIAL PRIMARY KEY,
    title VARCHAR(64) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.seniority (
    seniority_id SMALLSERIAL PRIMARY KEY,
    seniority VARCHAR(64) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.jobs_dim (
    job_id BIGSERIAL PRIMARY KEY,
    source_job_id TEXT UNIQUE NOT NULL,
    posted_at TIMESTAMP NOT NULL,
    company_name VARCHAR(512) NOT NULL,
    job_title VARCHAR(512) NOT NULL,
    description TEXT NOT NULL,
    description_tokens TEXT[] NOT NULL,
    source VARCHAR(512) NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.tag_jobs_fact (
    tag_id BIGINT NOT NULL,
    job_id BIGINT NOT NULL,
    job_title_id SMALLINT NOT NULL,
    seniority_id SMALLINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tag_id, job_id),
    CONSTRAINT fk_tags FOREIGN KEY (tag_id) REFERENCES warehouse.tags_dim (tag_id) ON DELETE CASCADE,
    CONSTRAINT fk_jobs FOREIGN KEY (job_id) REFERENCES warehouse.jobs_dim (job_id) ON DELETE CASCADE,
    CONSTRAINT fk_titles FOREIGN KEY (job_title_id) REFERENCES warehouse.titles (title_id) ON DELETE CASCADE,
    CONSTRAINT fk_seniority FOREIGN KEY (seniority_id) REFERENCES warehouse.seniority (seniority_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS staging.gsearch_jobs (
    job_id BIGSERIAL PRIMARY KEY,
    title VARCHAR(512) NOT NULL,
    company_name VARCHAR(512) NOT NULL,
    location VARCHAR(512) NOT NULL,
    via VARCHAR(512) NOT NULL,
    description TEXT NOT NULL,
    source_job_id TEXT UNIQUE NOT NULL,
    date_time TIMESTAMP,
    -- enriched
    cleaned_title VARCHAR(64) NOT NULL, 
    cleaned_seniority VARCHAR(64) NOT NULL, 
    description_tokens TEXT[] NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tags_embedding ON warehouse.tags_dim USING ivfflat (embedding);