\connect test

CREATE VIEW musicbrainz.edit_summary AS
 SELECT e.id,
    e.editor,
    e.type,
    e.status
   FROM musicbrainz.edit e
  GROUP BY e.id;

CREATE MATERIALIZED VIEW musicbrainz.medium_summary AS
 SELECT am.id,
    am.medium,
    am.alternative_release,
    am.name
   FROM musicbrainz.alternative_medium am
  GROUP BY am.id
  WITH NO DATA;
