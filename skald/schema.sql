CREATE TABLE score_history(
	id		SERIAL PRIMARY KEY NOT NULL,
	space 		TEXT NOT NULL,
	record_id	TEXT,
	request_id	TEXT NOT NULL,
	received	BIGINT NOT NULL,
	duration	BIGINT NOT NULL,
	model_name	TEXT,
	model_version	INTEGER,
	totality	TEXT,
	request		TEXT,
	response	TEXT
)