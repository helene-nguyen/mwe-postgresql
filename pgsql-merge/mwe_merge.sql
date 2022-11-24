\set ECHO ALL

DROP DATABASE IF EXISTS mwe_merge;

CREATE DATABASE mwe_merge;

\c mwe_merge

--============================================--
-- Starter conditions
--============================================--
DROP TABLE "user", "article";

CREATE TABLE IF NOT EXISTS "user"(
id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
name TEXT
);

CREATE TABLE IF NOT EXISTS "article"(
	id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, 
	user_id INT,
	content TEXT,
	created_at TIMESTAMPTZ
);

ALTER TABLE IF EXISTS "article"
    ADD FOREIGN KEY ("user_id")
    REFERENCES "user" ("id") 
    MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


INSERT INTO "user" ("name")
(
	SELECT('name' || ' ' || serie_nb)
	FROM generate_series( 1, 10) as serie_nb
);

INSERT INTO article ("user_id", "content", "created_at") 
(
	SELECT
		(SELECT floor(random() * (serie_nb + 1) + 1)::int), 
		('task' || ' ' || serie_nb), 
		(now() + interval '23 hours') 
	FROM generate_series( 1, 10) as serie_nb
);

TABLE article ORDER BY id;
TABLE "user" ORDER BY id;

--============================================--
-- Test merge with WHERE clause (will fail)
--============================================--
MERGE INTO article A
USING "user" U
ON U.id = A.user_id
WHEN MATCHED THEN 
	UPDATE SET content = 'For this user'
	WHERE U.id = 1
WHEN NOT MATCHED THEN
	INSERT ("content")
	VALUES ('task test');

-- When check value of all of articles, it creates 5 'task test' for the 5 users that not create any task
-- And when existing, the value will change
-- The WHERE clause will not work here

--============================================--
-- Test merge with AND condition (will success)
--============================================--

MERGE INTO article A
USING "user" U
ON U.id = A.user_id
WHEN MATCHED 
	AND U.id = 1
THEN 
	UPDATE SET content = 'Changes for this user'
WHEN NOT MATCHED 
	AND U.id = 1
THEN
	INSERT ("user_id","content", "created_at")
	VALUES (U.id::int,'Add a task', now());

-- It works now
-- Query explanation : 
-- Update the content if user_id = whatever match, if not, insert content for another user where id is whatever

--============================================--
-- Test merge with AND condition (will success)
--============================================--

MERGE INTO article A
USING "user" U
ON U.id = A.user_id
WHEN MATCHED 
	AND U.id = 3
THEN 
	UPDATE SET content = 'Changes for this user'
WHEN NOT MATCHED 
	AND U.id != 1 -- add a task, for each user that not match
THEN
	INSERT ("user_id","content", "created_at")
	VALUES (U.id::int,'Add a task', now());

TABLE article ORDER BY id;

--============================================--
-- Test with DELETE clause
--============================================--

MERGE INTO article A
USING "user" U
ON U.id = A.user_id
WHEN MATCHED 
	AND U.id = 1
THEN 
	UPDATE SET content = 'Changes for user one'
WHEN NOT MATCHED 
	AND U.id = 1
THEN
	INSERT ("user_id","content", "created_at")
	VALUES (U.id::int,'Add a task', now())
WHEN MATCHED 
THEN DELETE;

-- DELETE will delete all rows that not match with first condition

--============================================--
-- Test with DELETE clause
--============================================--

MERGE INTO article A
USING "user" U
ON U.id = A.user_id
WHEN MATCHED 
	AND U.id = 1
THEN 
	UPDATE SET content = 'Changes for user one'
WHEN NOT MATCHED 
	AND U.id = 1
THEN
	INSERT ("user_id","content", "created_at")
	VALUES (U.id::int,'Add a task', now())
WHEN MATCHED 
	AND U.id = 9
THEN
	DELETE;

-- Update content where the specified user match
-- Delete when match the user id whatever you want