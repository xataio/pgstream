--
-- PostgreSQL database dump
--

\restrict ddneeN6Oxl0ecekB9eBT4Gf3pl0KK8idsoGfccvrHpbmW08qS8rI99Mb8UlV0pK


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

--
-- Name: btree_gist; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;


--
-- Name: EXTENSION btree_gist; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION btree_gist IS 'support for indexing common datatypes in GiST';


SET default_tablespace = '';

--
-- Name: parts; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.parts (
    id integer NOT NULL,
    val text
)
PARTITION BY RANGE (id);


ALTER TABLE public.parts OWNER TO postgres;

SET default_table_access_method = heap;

--
-- Name: parts_0; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.parts_0 (
    id integer NOT NULL,
    val text
);


ALTER TABLE public.parts_0 OWNER TO postgres;

--
-- Name: parts_1; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.parts_1 (
    id integer NOT NULL,
    val text
);


ALTER TABLE public.parts_1 OWNER TO postgres;

--
-- Name: rooms; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.rooms (
    id integer NOT NULL,
    room integer,
    during tsrange,
    name text,
    email text,
    val numeric,
    CONSTRAINT rooms_room_positive CHECK ((room >= 0)),
    CONSTRAINT rooms_val_positive CHECK ((val >= (0)::numeric))
);


ALTER TABLE public.rooms OWNER TO postgres;

--
-- Name: parts_0; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.parts ATTACH PARTITION public.parts_0 FOR VALUES FROM (0) TO (10);


--
-- Name: parts_1; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.parts ATTACH PARTITION public.parts_1 FOR VALUES FROM (10) TO (20);


--
-- Name: rooms rooms_email_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rooms
    ADD CONSTRAINT rooms_email_key UNIQUE (email);

ALTER TABLE public.rooms CLUSTER ON rooms_email_key;


--
-- Name: rooms rooms_no_overlap; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.rooms
    ADD CONSTRAINT rooms_no_overlap EXCLUDE USING gist (room WITH =, during WITH &&);


--
-- Name: parts_val_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX parts_val_idx ON ONLY public.parts USING btree (val);


--
-- Name: parts_0_val_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX parts_0_val_idx ON public.parts_0 USING btree (val);


--
-- Name: parts_1_val_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX parts_1_val_idx ON public.parts_1 USING btree (val);


--
-- Name: rooms_expr_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX rooms_expr_idx ON public.rooms USING btree (lower(name), upper(email), ((val * (2)::numeric)), COALESCE(name, email, 'a-fairly-long-default-value'::text));


--
-- Name: rooms_partial_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX rooms_partial_idx ON public.rooms USING btree (name) WHERE ((val > (100)::numeric) AND (name IS NOT NULL) AND (email IS NOT NULL));


--
-- Name: INDEX rooms_partial_idx; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON INDEX public.rooms_partial_idx IS 'partial';


--
-- Name: parts_0_val_idx; Type: INDEX ATTACH; Schema: public; Owner: postgres
--

ALTER INDEX public.parts_val_idx ATTACH PARTITION public.parts_0_val_idx;


--
-- Name: parts_1_val_idx; Type: INDEX ATTACH; Schema: public; Owner: postgres
--

ALTER INDEX public.parts_val_idx ATTACH PARTITION public.parts_1_val_idx;


--
-- PostgreSQL database dump complete
--

\unrestrict ddneeN6Oxl0ecekB9eBT4Gf3pl0KK8idsoGfccvrHpbmW08qS8rI99Mb8UlV0pK

