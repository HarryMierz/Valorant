-- Table: match_statistics.match_map_player

DROP TABLE IF EXISTS match_statistics.match_map_player;

CREATE TABLE IF NOT EXISTS match_statistics.match_map_player
(
    match_map_player_id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    match_id integer,
    map_name character varying,
    player_name character varying,
    acs_overall integer,
    acs_attack integer,
    acs_defense integer,
    kills_overall integer,
    kills_attack integer,
    kills_defense integer,
    deaths_overall integer,
    deaths_attack integer,
    deaths_defense integer,
    assists_overall integer,
    assists_attack integer,
    assists_defense integer,
    kast_overall integer,
    kast_attack integer,
    kast_defense integer,
    adr_overall integer,
    adr_attack integer,
    adr_defense integer,
    headshot_percentage_overall integer,
    headshot_percentage_attack integer,
    headshot_percentage_defense integer,
    first_kills_overall integer,
    first_kills_attack integer,
    first_kills_defense integer,
    first_deaths_overall integer,
    first_deaths_attack integer,
    first_deaths_defense integer,
    CONSTRAINT match_map_player_pkey PRIMARY KEY (match_map_player_id),
    CONSTRAINT fd_match_match_map_player FOREIGN KEY (match_id)
        REFERENCES match_statistics.match (match_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION

)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS match_statistics.match_map_player
    OWNER to batch;