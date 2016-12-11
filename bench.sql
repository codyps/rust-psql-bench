CREATE TABLE users (
	name TEXT PRIMARY KEY NOT NULL,
	hair_color TEXT
);

CREATE TABLE posts (
	user_id integer NOT NULL,
	title TEXT NOT NULL,
	body TEXT
);

