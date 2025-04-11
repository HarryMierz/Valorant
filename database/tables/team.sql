-- Table: match_statistics.team

-- DROP TABLE IF EXISTS match_statistics.team;

CREATE TABLE IF NOT EXISTS match_statistics.team
(
    team_id bigint NOT NULL DEFAULT nextval('match_statistics.team_team_id_seq'::regclass),
    team_name character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT team_pkey PRIMARY KEY (team_id, team_name),
    CONSTRAINT u_team_id UNIQUE (team_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS match_statistics.team
    OWNER to batch;