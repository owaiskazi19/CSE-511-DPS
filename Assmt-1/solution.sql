CREATE TABLE users
(
    userid INT PRIMARY KEY NOT NULL,
	name TEXT NOT NULL
);

CREATE TABLE movies
(
    movieid INT PRIMARY KEY NOT NULL,
	title TEXT NOT NULL
);

CREATE TABLE taginfo
(
    tagid INT PRIMARY KEY NOT NULL,
	content TEXT NOT NULL
);

CREATE TABLE genres
(
    genreid INT PRIMARY KEY NOT NULL,
	name TEXT NOT NULL
);

CREATE TABLE ratings
(
    userid INT NOT NULL,
	movieid INT NOT NULL,
	rating NUMERIC NOT NULL,
	"timestamp" BIGINT NOT NULL,
	CHECK(rating>=0 and rating<=5),
	PRIMARY KEY(userid, movieid),
	FOREIGN KEY(userid) REFERENCES users(userid),
	FOREIGN KEY (movieid) REFERENCES movies(movieid)
);

CREATE TABLE tags
(
    userid INT NOT NULL,
	movieid INT NOT NULL,
	tagid INT NOT NULL,
	"timestamp" BIGINT NOT NULL,
	PRIMARY KEY(userid, movieid, tagid),
	FOREIGN KEY(userid) REFERENCES users(userid),
	FOREIGN KEY (movieid) REFERENCES movies(movieid),
	FOREIGN KEY (tagid) REFERENCES taginfo(tagid)
);

CREATE TABLE hasagenre
(
	movieid INT NOT NULL,
	genreid INT NOT NULL,
	PRIMARY KEY(movieid, genreid),
	FOREIGN KEY (movieid) REFERENCES movies(movieid),
	FOREIGN KEY (genreid) REFERENCES genres(genreid)
);
