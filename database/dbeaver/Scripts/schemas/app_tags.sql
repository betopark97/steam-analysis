CREATE TABLE staging.app_tags (
	appid int4 NOT NULL,
	tags _text NULL,
	updated_at timestamp DEFAULT now() NULL,
	CONSTRAINT app_tags_pkey PRIMARY KEY (appid)
);