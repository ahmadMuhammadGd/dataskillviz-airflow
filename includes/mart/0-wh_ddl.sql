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
    embedding VECTOR(300) NOT NULL
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

CREATE TABLE IF NOT EXISTS warehouse.tags_jobs_fact (
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


CREATE TABLE IF NOT EXISTS warehouse.reduced_tags_jobs_fact (
    job_id BIGSERIAL PRIMARY KEY,
    tags_list INT[],
    job_title_id SMALLINT,
    seniority_id SMALLINT,
    updated_at TIMESTAMP,
    CONSTRAINT fk_titles FOREIGN KEY (job_title_id) REFERENCES warehouse.titles (title_id) ON DELETE CASCADE,
    CONSTRAINT fk_seniority FOREIGN KEY (seniority_id) REFERENCES warehouse.seniority (seniority_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS warehouse.tags_fp_growth (
    source_tag BIGINT NOT NULL,
    target_tag BIGINT NOT NULL,
    m_support FLOAT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_DATE,
    CONSTRAINT fk_source_tag FOREIGN KEY (source_tag) REFERENCES warehouse.tags_dim (tag_id) ON DELETE CASCADE,
    CONSTRAINT fk_target_tag FOREIGN KEY (target_tag) REFERENCES warehouse.tags_dim (tag_id) ON DELETE CASCADE,
    PRIMARY KEY (source_tag, target_tag)
);

DROP TABLE warehouse.tags_fp_growth ;
INSERT INTO warehouse.titles (title)
SELECT seed_titles
FROM (
    VALUES
        ('Data Engineer'),
        ('Data Analyst'),
        ('Data Scientist'),
        ('Not Specified')
) AS seed(seed_titles)
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.titles t WHERE t.title = seed.seed_titles
);

INSERT INTO warehouse.seniority (seniority)
SELECT seed_seniority
FROM (
    VALUES
        ('Junior'),
        ('Mid-level'),
        ('Senior'),
        ('Not Specified')
) AS seed(seed_seniority)
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.seniority s WHERE s.seniority = seed.seed_seniority
);