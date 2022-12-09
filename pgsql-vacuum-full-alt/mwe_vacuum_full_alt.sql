
--=======================
-- Start
--=======================
\set ECHO ALL

DROP DATABASE IF EXISTS mwe_vacuum_full;

CREATE DATABASE mwe_vacuum_full;

\c mwe_vacuum_full

--=======================
-- Model
--=======================
DROP TABLE "category", "article", "user";

CREATE TABLE IF NOT EXISTS "user"(
id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
name TEXT
);

CREATE TABLE IF NOT EXISTS "article"(
	id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, 
	user_id INT NOT NULL,
	content TEXT
);

CREATE TABLE IF NOT EXISTS "category"(
	id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, 
	article_id INT NOT NULL,
	content TEXT
);

ALTER TABLE IF EXISTS "article"
    ADD CONSTRAINT fk_article_user
	FOREIGN KEY ("user_id")
    REFERENCES "user" ("id") 
    ON DELETE CASCADE;

ALTER TABLE IF EXISTS "category"
    ADD CONSTRAINT fk_category_article
    FOREIGN KEY ("article_id")
    REFERENCES "article" ("id") 
    ON DELETE CASCADE;


--================================================
-- Create indexes
--================================================

-- DROP INDEX article_idx, category_idx;
CREATE INDEX CONCURRENTLY user_idx ON "user"("name");
CREATE INDEX CONCURRENTLY article_idx ON "article"("content");
CREATE INDEX CONCURRENTLY category_idx ON "category"("content");

--================================================
-- Insert data
--================================================

INSERT INTO "user" ("name")
(
	SELECT('name' || ' ' || serie_nb)
	FROM generate_series( 1, 50000) as serie_nb
);

INSERT INTO article ("user_id", "content")
(
	SELECT
		(SELECT floor((random() * serie_nb + 1))::int), 
		('task' || ' ' || serie_nb) 
	FROM generate_series( 1, 50000) as serie_nb
);

INSERT INTO category ("article_id", "content") 
(
	SELECT
		(SELECT floor(random() * serie_nb + 1)::int), 
		('category' || ' ' || serie_nb) 
	FROM generate_series( 1, 50000) as serie_nb
);


-- TABLE "user" LIMIT 100;
-- TABLE article;
-- TABLE category;

-- \d article
-- \d category


--=======================
-- List all indexes
--=======================

-- Solution 1

-- SELECT * FROM pg_indexes WHERE tablename = 'article' \gx

-- Solution 2

-- SELECT
--     indexname,
--     indexdef
-- FROM
--     pg_indexes
-- WHERE
--     tablename = 'user';

-- SELECT
--     indexname,
--     indexdef
-- FROM
--     pg_indexes
-- WHERE
--     tablename = 'article';
	
-- SELECT
--     indexname,
--     indexdef
-- FROM
--     pg_indexes
-- WHERE
--     tablename = 'category';

--=======================
-- Check storage density
--=======================

CREATE EXTENSION pgstattuple;

\dx pgstattuple

--================================================
-- Create function to collect tg constraints  
--================================================


CREATE OR REPLACE FUNCTION rel_trigger (tables text[])
RETURNS TABLE (rel_oid oid, rel_name name, trigger_name json)
AS $$

BEGIN
-- RAISE NOTICE '%', tables;
RETURN QUERY(SELECT REL.oid, REL.relname, json_agg(tgname)
FROM pg_class as REL
JOIN pg_trigger as TG
ON TG.tgrelid = REL.oid
WHERE REL.relname = any(tables)
GROUP BY REL.oid);

END;

$$ LANGUAGE plpgsql;

SELECT * FROM rel_trigger(array['user','article','category']::text[]);

--==============================================================
-- Create function to collect size & path information tables  
--==============================================================
DROP FUNCTION table_details;

CREATE OR REPLACE FUNCTION table_details (name TEXT)
RETURNS TABLE(
	oid_info oid, 
	rel_name name,
	rel_pages INT,
	tuple_percent DOUBLE PRECISION,
	avg_leaf_density DOUBLE PRECISION,
	rel_size TEXT,
	index_size TEXT,
	filepath TEXT,
	idx_filepath TEXT) AS $$

BEGIN

RETURN QUERY(SELECT 
	oid, 
	relname,
	relpages,
	T.tuple_percent, 
	I.avg_leaf_density, 
	pg_size_pretty(pg_table_size(name)), 
	pg_size_pretty(pg_indexes_size(name)),
	pg_relation_filepath(name),
	pg_relation_filepath(name || '_idx')
FROM pg_class, pgstattuple(name) T, pgstatindex(name || '_idx') I
WHERE relname  = name);

END;

$$ LANGUAGE plpgsql;


--================================================
-- Details BEFORE DELETE
--================================================
\x
-- use the function
SELECT * FROM table_details('user');
SELECT * FROM table_details('article');
SELECT * FROM table_details('category');

\x

--================================================
-- Delete datas
--================================================

DELETE FROM "category" WHERE id <= 30000;
DELETE FROM "article" WHERE id <= 10000;
DELETE FROM "user" WHERE id <= 10000;

--================================================
-- Details AFTER DELETE
--================================================

\x
-- use the function
SELECT * FROM table_details('user');
SELECT * FROM table_details('article');
SELECT * FROM table_details('category');

\x

-- Here you can check details about density of DATABASE


-- --=================================
-- Add data at the end of the table
-- --=================================

INSERT INTO "user" ("name")
(
	SELECT('name' || ' ' || serie_nb)
	FROM generate_series( 1, 1000) as serie_nb
);

INSERT INTO article ("user_id", "content")
(
	SELECT
		(SELECT a.user_id 
		 FROM article a 
		 JOIN "user" u 
		 ON a.user_id = u.id 
		 LIMIT 1 ), 
		('task' || ' ' || serie_nb) 
	FROM generate_series( 1, 1000) as serie_nb
);

INSERT INTO category ("article_id", "content") 
(
	SELECT
		(SELECT c.article_id 
		 FROM category c 
		 JOIN article a 
		 ON c.article_id = a.id 
		 LIMIT 1 ), 
		('category' || ' ' || serie_nb) 
	FROM generate_series( 1, 1000) as serie_nb
);

