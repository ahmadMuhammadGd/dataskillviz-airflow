INSERT INTO warehouse.tags_dim (tag, embedding)
VALUES %s
ON CONFLICT (tag) DO NOTHING;