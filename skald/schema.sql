CREATE TABLE ScoreHistory(
	id		SERIAL PRIMARY KEY NOT NULL,
	space 		TEXT NOT NULL,
	recordId	TEXT,
	requestId	TEXT NOT NULL,
	received	BIGINT NOT NULL,
	duration	BIGINT NOT NULL,
	modelName	TEXT,
	modelVersion	INTEGER,
	totality	TEXT,
	request		TEXT,
	response	TEXT