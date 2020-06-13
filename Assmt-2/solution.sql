-- Query 1
CREATE TABLE query1 AS SELECT genres.name,  count(movies.movieid) AS moviecount 
FROM movies INNER JOIN hasagenre 
ON hasagenre.movieid = movies.movieid INNER JOIN 
genres ON hasagenre.genreid = genres.genreid 
GROUP BY genres.name;

-- Query 2
CREATE TABLE query2 AS SELECT genres.name,  avg(ratings.rating) 
AS rating FROM genres 
INNER JOIN hasagenre ON hasagenre.genreid = genres.genreid 
INNER JOIN ratings ON hasagenre.movieid = ratings.movieid 
GROUP BY genres.name;

-- Query 3
CREATE TABLE query3 AS SELECT movies.title,  count(ratings.rating)  
AS countofratings FROM movies 
INNER JOIN ratings ON movies.movieid = ratings.movieid 
GROUP BY movies.title HAVING count(ratings.rating)>=10;

-- Query 4
CREATE TABLE query4 AS SELECT movies.movieid, movies.title 
FROM movies INNER JOIN hasagenre ON movies.movieid = hasagenre.movieid 
INNER JOIN genres ON genres.genreid = hasagenre.genreid 
WHERE genres.name = 'Comedy'; 

-- Query 5
CREATE TABLE query5 AS SELECT movies.title,  avg(ratings.rating) 
AS average FROM movies  
INNER JOIN ratings ON movies.movieid = ratings.movieid 
GROUP BY movies.title;

-- Query 6
CREATE TABLE query6 AS SELECT avg(ratings.rating) 
AS average FROM ratings 
INNER JOIN hasagenre  ON hasagenre.movieid = ratings.movieid 
INNER JOIN genres ON genres.genreid = hasagenre.genreid 
WHERE genres.name = 'Comedy';

-- Query 7
CREATE TABLE query7 AS SELECT avg(ratings.rating) 
AS average FROM ratings 
INNER JOIN hasagenre  ON hasagenre.movieid = ratings.movieid 
INNER JOIN genres ON genres.genreid = hasagenre.genreid 
WHERE genres.name = 'Comedy' AND ratings.movieid IN 
(SELECT ratings.movieid FROM ratings INNER JOIN hasagenre ON ratings.movieid = hasagenre.movieid 
INNER JOIN genres ON genres.genreid = hasagenre.genreid WHERE genres.name = 'Romance');

-- Query 8
CREATE TABLE query8 AS SELECT avg(ratings.rating) 
AS average FROM ratings 
INNER JOIN hasagenre  ON hasagenre.movieid = ratings.movieid 
INNER JOIN genres ON genres.genreid = hasagenre.genreid WHERE genres.name = 'Romance' AND 
ratings.movieid NOT IN (SELECT ratings.movieid FROM ratings INNER JOIN hasagenre ON 
ratings.movieid = hasagenre.movieid 
INNER JOIN genres ON genres.genreid = hasagenre.genreid WHERE genres.name = 'Comedy');

-- Query 9
-- \set v1 2;
CREATE TABLE query9 AS SELECT ratings.movieid, ratings.rating FROM ratings WHERE ratings.userid = :v1;

-- Query 10
CREATE VIEW v1 AS
SELECT r.movieid, r.rating
FROM ratings r
WHERE r.userid = :v1;

CREATE VIEW v2 AS
SELECT movieid, avg(rating) AS rating
FROM ratings
GROUP BY movieid;

CREATE TABLE similarity AS
SELECT t1.movieid AS movieid1, t2.movieid AS movieid2,
(1.0 - ABS(t1.rating - t2.rating)/5.0) AS sim
FROM v2 AS t1 CROSS JOIN v2 AS t2;

CREATE TABLE prediction AS
SELECT m.movieid1 AS candidate,
  CASE SUM(m.sim) WHEN 0.0 THEN 0.0
                  ELSE SUM(m.sim*u.rating)/SUM(m.sim)
  END
AS predictionscore
FROM similarity m, v1 u
WHERE m.movieid2 = u.movieid
AND m.movieid1 NOT IN (SELECT movieid FROM v1)
GROUP BY m.movieid1 ORDER BY predictionscore DESC;


CREATE TABLE recommendation AS
SELECT title
FROM movies, prediction
WHERE movies.movieid = prediction.candidate
AND prediction.predictionscore>3.9;




