-- Table: match_statistics.player

-- DROP TABLE IF EXISTS match_statistics.player;

CREATE TABLE IF NOT EXISTS match_statistics.player
(
    player_id bigint NOT NULL DEFAULT nextval('match_statistics.player_player_id_seq'::regclass),
    player_name character varying COLLATE pg_catalog."default",
    team_id integer,
    CONSTRAINT player_pkey PRIMARY KEY (player_id),
    CONSTRAINT pk_team_player FOREIGN KEY (team_id)
        REFERENCES match_statistics.team (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS match_statistics.player
    OWNER to batch;