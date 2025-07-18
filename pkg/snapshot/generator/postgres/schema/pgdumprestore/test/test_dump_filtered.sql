--
-- PostgreSQL database dump
--

-- Dumped from database version 17.2
-- Dumped by pg_dump version 17.5

\connect test

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE IF EXISTS ONLY musicbrainz.alternative_medium DROP CONSTRAINT IF EXISTS alternative_medium_fk_medium;
ALTER TABLE IF EXISTS ONLY musicbrainz.alternative_medium DROP CONSTRAINT IF EXISTS alternative_medium_fk_alternative_release;
DROP TRIGGER IF EXISTS a_del_alternative_release ON musicbrainz.alternative_release;
DROP TRIGGER IF EXISTS a_del_alternative_medium_track ON musicbrainz.alternative_medium_track;
DROP INDEX IF EXISTS musicbrainz.alternative_release_idx_artist_credit;
DROP INDEX IF EXISTS musicbrainz.alternative_medium_idx_alternative_release;
ALTER TABLE IF EXISTS ONLY musicbrainz.alternative_medium_track DROP CONSTRAINT IF EXISTS alternative_medium_track_pkey;
ALTER TABLE IF EXISTS ONLY musicbrainz.alternative_medium DROP CONSTRAINT IF EXISTS alternative_medium_pkey;
ALTER TABLE IF EXISTS musicbrainz.alternative_release ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS musicbrainz.alternative_medium ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS musicbrainz.alternative_medium_id_seq;
DROP TABLE IF EXISTS musicbrainz.alternative_medium;
DROP TABLE IF EXISTS musicbrainz.edit;
DROP TEXT SEARCH CONFIGURATION IF EXISTS musicbrainz.mb_simple;
DROP AGGREGATE IF EXISTS musicbrainz.median(integer);
DROP FUNCTION IF EXISTS musicbrainz.a_del_alternative_medium_track();
DROP FUNCTION IF EXISTS musicbrainz._median(integer[]);
DROP TYPE IF EXISTS musicbrainz.edit_note_status;
DROP TYPE IF EXISTS musicbrainz.cover_art_presence;
DROP EXTENSION IF EXISTS cube;
DROP COLLATION IF EXISTS musicbrainz.musicbrainz;
DROP SCHEMA IF EXISTS musicbrainz;
--
-- Name: musicbrainz; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA musicbrainz;


ALTER SCHEMA musicbrainz OWNER TO postgres;

--
-- Name: musicbrainz; Type: COLLATION; Schema: musicbrainz; Owner: postgres
--

CREATE COLLATION musicbrainz.musicbrainz (provider = icu, locale = 'und-u-kf-lower-kn-true');


ALTER COLLATION musicbrainz.musicbrainz OWNER TO postgres;

--
-- Name: cube; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS cube WITH SCHEMA public;


--
-- Name: EXTENSION cube; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION cube IS 'data type for multidimensional cubes';


--
-- Name: cover_art_presence; Type: TYPE; Schema: musicbrainz; Owner: postgres
--

CREATE TYPE musicbrainz.cover_art_presence AS ENUM (
    'absent',
    'present',
    'darkened'
);


ALTER TYPE musicbrainz.cover_art_presence OWNER TO postgres;

--
-- Name: edit_note_status; Type: TYPE; Schema: musicbrainz; Owner: postgres
--

CREATE TYPE musicbrainz.edit_note_status AS ENUM (
    'deleted',
    'edited'
);


ALTER TYPE musicbrainz.edit_note_status OWNER TO postgres;

--
-- Name: _median(integer[]); Type: FUNCTION; Schema: musicbrainz; Owner: postgres
--

CREATE FUNCTION musicbrainz._median(integer[]) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $_$
  WITH q AS (
      SELECT val
      FROM unnest($1) val
      WHERE VAL IS NOT NULL
      ORDER BY val
  )
  SELECT val
  FROM q
  LIMIT 1
  -- Subtracting (n + 1) % 2 creates a left bias
  OFFSET greatest(0, floor((select count(*) FROM q) / 2.0) - ((select count(*) + 1 FROM q) % 2));
$_$;


ALTER FUNCTION musicbrainz._median(integer[]) OWNER TO postgres;

--
-- Name: a_del_alternative_medium_track(); Type: FUNCTION; Schema: musicbrainz; Owner: postgres
--

CREATE FUNCTION musicbrainz.a_del_alternative_medium_track() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM dec_ref_count('alternative_track', OLD.alternative_track, 1);
    RETURN NULL;
END;
$$;


ALTER FUNCTION musicbrainz.a_del_alternative_medium_track() OWNER TO postgres;

--
-- Name: median(integer); Type: AGGREGATE; Schema: musicbrainz; Owner: postgres
--

CREATE AGGREGATE musicbrainz.median(integer) (
    SFUNC = array_append,
    STYPE = integer[],
    INITCOND = '{}',
    FINALFUNC = musicbrainz._median
);


ALTER AGGREGATE musicbrainz.median(integer) OWNER TO postgres;

--
-- Name: mb_simple; Type: TEXT SEARCH CONFIGURATION; Schema: musicbrainz; Owner: postgres
--

CREATE TEXT SEARCH CONFIGURATION musicbrainz.mb_simple (
    PARSER = pg_catalog."default" );

ALTER TEXT SEARCH CONFIGURATION musicbrainz.mb_simple
    ADD MAPPING FOR asciiword WITH simple;


ALTER TEXT SEARCH CONFIGURATION musicbrainz.mb_simple OWNER TO postgres;

--
-- Name: edit; Type: TABLE; Schema: musicbrainz; Owner: postgres
--

CREATE TABLE musicbrainz.edit (
    id integer NOT NULL,
    editor integer NOT NULL,
    type smallint NOT NULL,
    status smallint NOT NULL,
    autoedit smallint DEFAULT 0 NOT NULL,
    open_time timestamp with time zone DEFAULT now(),
    close_time timestamp with time zone,
    expire_time timestamp with time zone NOT NULL,
    language integer,
    quality smallint DEFAULT 1 NOT NULL
);


ALTER TABLE musicbrainz.edit OWNER TO postgres;

--
-- Name: alternative_medium; Type: TABLE; Schema: musicbrainz; Owner: postgres
--

CREATE TABLE musicbrainz.alternative_medium (
    id integer NOT NULL,
    medium integer NOT NULL,
    alternative_release integer NOT NULL,
    name character varying,
    CONSTRAINT alternative_medium_name_check CHECK (((name)::text <> ''::text))
);


ALTER TABLE musicbrainz.alternative_medium OWNER TO postgres;


CREATE TABLE musicbrainz."Alternative_medium" (
    id integer NOT NULL,
    medium integer NOT NULL,
    alternative_release integer NOT NULL,
    name character varying,
    CONSTRAINT alternative_medium_name_check CHECK (((name)::text <> ''::text))
);

--
-- Name: alternative_medium_id_seq; Type: SEQUENCE; Schema: musicbrainz; Owner: postgres
--

CREATE SEQUENCE musicbrainz.alternative_medium_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE musicbrainz."Alternative_medium_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE musicbrainz.alternative_medium_id_seq OWNER TO postgres;

--
-- Name: alternative_medium id; Type: DEFAULT; Schema: musicbrainz; Owner: postgres
--

ALTER TABLE ONLY musicbrainz.alternative_medium ALTER COLUMN id SET DEFAULT nextval('musicbrainz.alternative_medium_id_seq'::regclass);


--
-- Name: alternative_release id; Type: DEFAULT; Schema: musicbrainz; Owner: postgres
--

ALTER TABLE ONLY musicbrainz.alternative_release ALTER COLUMN id SET DEFAULT nextval('musicbrainz.alternative_release_id_seq'::regclass);


--
-- Name: alternative_medium alternative_medium_pkey; Type: CONSTRAINT; Schema: musicbrainz; Owner: postgres
--



--
-- Name: alternative_medium_track alternative_medium_track_pkey; Type: CONSTRAINT; Schema: musicbrainz; Owner: postgres
--




ALTER TABLE ONLY musicbrainz.alternative_medium_track
    ADD COLUMN alternative_track integer NOT NULL;

--
-- Name: area_alias_idx_txt; Type: INDEX; Schema: musicbrainz; Owner: postgres
--




--
-- Name: area_alias_type_idx_gid; Type: INDEX; Schema: musicbrainz; Owner: postgres
--



--
-- Name: alternative_medium_track a_del_alternative_medium_track; Type: TRIGGER; Schema: musicbrainz; Owner: postgres
--




--
-- Name: alternative_release a_del_alternative_release; Type: TRIGGER; Schema: musicbrainz; Owner: postgres
--


--
-- Name: release apply_artist_release_group_pending_updates; Type: TRIGGER; Schema: musicbrainz; Owner: postgres
--




--
-- Name: release_group apply_artist_release_group_pending_updates; Type: TRIGGER; Schema: musicbrainz; Owner: postgres
--


--
-- Name: alternative_medium alternative_medium_fk_alternative_release; Type: FK CONSTRAINT; Schema: musicbrainz; Owner: postgres
--



--
-- Name: alternative_medium alternative_medium_fk_medium; Type: FK CONSTRAINT; Schema: musicbrainz; Owner: postgres
--


--
-- PostgreSQL database dump complete
--
