CREATE OR REPLACE VIEW frequency_report AS 
SELECT 
    f.tag_id
    , td.tag
    , sl.seniority
    , tl.title 
    , COUNT(*) as occurance
FROM
    warehouse.tags_jobs_fact f 
LEFT JOIN 
    warehouse.tags_dim td 
ON
    f.tag_id = td.tag_id 
LEFT JOIN 
    warehouse.seniority sl  
ON
    f.seniority_id = sl.seniority_id
LEFT JOIN  
    warehouse.titles tl  
ON 
    f.job_title_id = tl.title_id
GROUP BY
    f.tag_id
    , td.tag
    , sl.seniority
    , tl.title