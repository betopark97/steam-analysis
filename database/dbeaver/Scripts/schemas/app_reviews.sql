CREATE TABLE staging.app_reviews (
	appid int4 NOT NULL,
	html jsonb NULL,
	review_score jsonb NULL,
	updated_at timestamp DEFAULT now() NULL,
	CONSTRAINT app_reviews_pkey PRIMARY KEY (appid)
);