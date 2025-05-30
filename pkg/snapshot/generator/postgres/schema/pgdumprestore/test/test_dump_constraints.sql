\connect test

ALTER TABLE ONLY musicbrainz.alternative_medium
    ADD CONSTRAINT alternative_medium_pkey PRIMARY KEY (id);

ALTER TABLE ONLY musicbrainz.alternative_medium_track
    ADD CONSTRAINT alternative_medium_track_pkey PRIMARY KEY (alternative_medium, track);

CREATE INDEX area_alias_idx_txt ON musicbrainz.area_alias USING gin (musicbrainz.mb_simple_tsvector((name)::text));

CREATE UNIQUE INDEX area_alias_type_idx_gid ON musicbrainz.area_alias_type USING btree (gid);

CREATE TRIGGER a_del_alternative_medium_track AFTER DELETE ON musicbrainz.alternative_medium_track FOR EACH ROW EXECUTE FUNCTION musicbrainz.a_del_alternative_medium_track();

CREATE TRIGGER a_del_alternative_release AFTER DELETE ON musicbrainz.alternative_release FOR EACH ROW EXECUTE FUNCTION musicbrainz.a_del_alternative_release_or_track();

CREATE CONSTRAINT TRIGGER apply_artist_release_group_pending_updates AFTER INSERT OR DELETE OR UPDATE ON musicbrainz.release DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION musicbrainz.apply_artist_release_group_pending_updates();

CREATE CONSTRAINT TRIGGER apply_artist_release_group_pending_updates AFTER INSERT OR DELETE OR UPDATE ON musicbrainz.release_group DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION musicbrainz.apply_artist_release_group_pending_updates();

ALTER TABLE ONLY musicbrainz.alternative_medium
    ADD CONSTRAINT alternative_medium_fk_alternative_release FOREIGN KEY (alternative_release) REFERENCES musicbrainz.alternative_release(id);

ALTER TABLE ONLY musicbrainz.alternative_medium
    ADD CONSTRAINT alternative_medium_fk_medium FOREIGN KEY (medium) REFERENCES musicbrainz.medium(id);
