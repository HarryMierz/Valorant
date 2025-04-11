-- Table: match_statistics.map

-- DROP TABLE IF EXISTS match_statistics.map;

CREATE TABLE IF NOT EXISTS match_statistics.map
(
    map_id integer NOT NULL DEFAULT nextval('match_statistics.map_map_id_seq'::regclass),
    map_name character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT map_pkey PRIMARY KEY (map_id, map_name),
    CONSTRAINT u_map_id UNIQUE (map_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS match_statistics.map
    OWNER to batch;