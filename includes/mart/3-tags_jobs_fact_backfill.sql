INSERT INTO warehouse.tags_jobs_fact (
    tag_id, 
    job_id, 
    job_title_id, 
    seniority_id, 
    updated_at
)
SELECT
    t.tag_id,
    g.job_id,
    ti.title_id,
    se.seniority_id,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    warehouse.jobs_dim g
LEFT JOIN 
    staging.gsearch_jobs sg
    ON g.source_job_id = sg.source_job_id
LEFT JOIN 
    warehouse.tags_dim t 
    ON t.tag = ANY(g.description_tokens)
LEFT JOIN 
    warehouse.titles ti 
    ON ti.title = sg.cleaned_title
JOIN 
    warehouse.seniority se 
    ON se.seniority = sg.cleaned_seniority 
WHERE
	t.tag_id IS NOT NULL
ON CONFLICT (tag_id, job_id) DO NOTHING; 
