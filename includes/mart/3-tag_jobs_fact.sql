INSERT INTO warehouse.tag_jobs_fact (tag_id, job_id, job_title_id, seniority_id, updated_at)
SELECT 
    t.tag_id,
    g.job_id,
    ti.title_id,
    se.seniority_id,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    staging.gsearch_jobs g
LEFT JOIN 
    warehouse.tags_dim t 
    ON t.tag = ANY(g.description_tokens)
LEFT JOIN 
    warehouse.titles ti 
    ON ti.title = g.cleaned_title
JOIN 
    warehouse.seniority se 
    ON se.seniority = g.cleaned_seniority 
WHERE
    g.source_job_id IS NOT NULL
	AND
	t.tag_id IS NOT NULL; 
