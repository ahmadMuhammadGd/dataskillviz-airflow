INSERT INTO warehouse.jobs_dim   
(
    source_job_id,
    posted_at,
    company_name,
    job_title,
    description,
    description_tokens,
    source
)
SELECT 
    src.source_job_id, 
    src.date_time AS posted_at, 
    src.company_name, 
    src.title AS job_title, 
    src.description, 
    src.description_tokens AS description_tokens, 
    src.via AS source
FROM 
    staging.gsearch_jobs src
LEFT JOIN 
    warehouse.jobs_dim dim 
ON 
    src.source_job_id = dim.source_job_id
WHERE 
    dim.job_id IS NULL; 