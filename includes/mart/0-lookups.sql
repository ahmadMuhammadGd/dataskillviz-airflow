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
