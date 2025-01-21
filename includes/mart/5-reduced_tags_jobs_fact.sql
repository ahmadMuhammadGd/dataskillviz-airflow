-- Active: 1736343585667@@pg-31a27cd9-ahmadmuhammadgd-1209.e.aivencloud.com@27695@vector_db_test
INSERT INTO warehouse.reduced_tags_jobs_fact (
    job_id,
    tags_list,
    job_title_id,
    seniority_id,
    updated_at
)
WITH new_jobs AS (
    SELECT 
        src.job_id,
        src.tag_id,
        src.job_title_id,
        src.seniority_id
    FROM
        warehouse.tags_jobs_fact AS src
    LEFT JOIN 
        warehouse.reduced_tags_jobs_fact AS dist
    ON
        src.job_id = dist.job_id
    WHERE
        dist.job_id IS NULL
)
SELECT 
    job_id,
    array_agg(tag_id::INT),
    job_title_id,
    seniority_id,
    current_timestamp
FROM 
    new_jobs
GROUP BY
    job_id,
    job_title_id,
    seniority_id