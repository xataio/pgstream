-- Fixture for object type filtering integration test
-- Creates objects across all filterable categories
-- Uses gen_random_uuid() instead of uuid_generate_v4() to avoid uuid-ossp dependency

BEGIN;

-- =============================================================================
-- SCHEMA
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================================
-- EXTENSIONS
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

-- =============================================================================
-- TYPES (TYPE, DOMAIN)
-- =============================================================================
CREATE TYPE app.status AS ENUM ('active', 'inactive', 'suspended', 'deleted');

CREATE TYPE app.address AS (
    street TEXT,
    city   TEXT,
    state  TEXT,
    zip    TEXT
);

CREATE DOMAIN app.email AS TEXT
    CHECK (VALUE ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$');

CREATE DOMAIN app.positive_int AS INTEGER
    CHECK (VALUE > 0);

-- =============================================================================
-- TABLES (TABLE, DEFAULT)
-- =============================================================================
CREATE TABLE app.users (
    id         SERIAL PRIMARY KEY,
    uuid       UUID DEFAULT gen_random_uuid() NOT NULL,
    username   VARCHAR(100) NOT NULL,
    email      app.email NOT NULL,
    status     app.status DEFAULT 'active' NOT NULL,
    address    app.address,
    score      app.positive_int DEFAULT 1,
    bio        TEXT,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT users_username_length CHECK (char_length(username) >= 2)
);

CREATE TABLE app.categories (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    slug        VARCHAR(200) NOT NULL,
    parent_id   INTEGER REFERENCES app.categories(id),
    sort_order  INTEGER DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE app.posts (
    id          SERIAL PRIMARY KEY,
    author_id   INTEGER NOT NULL REFERENCES app.users(id),
    category_id INTEGER REFERENCES app.categories(id),
    title       VARCHAR(500) NOT NULL,
    slug        VARCHAR(500) NOT NULL,
    body        TEXT NOT NULL,
    is_draft    BOOLEAN DEFAULT true NOT NULL,
    view_count  INTEGER DEFAULT 0 NOT NULL,
    published_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE app.tags (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE app.post_tags (
    post_id INTEGER NOT NULL REFERENCES app.posts(id) ON DELETE CASCADE,
    tag_id  INTEGER NOT NULL REFERENCES app.tags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);

CREATE TABLE app.comments (
    id        SERIAL PRIMARY KEY,
    post_id   INTEGER NOT NULL REFERENCES app.posts(id) ON DELETE CASCADE,
    user_id   INTEGER NOT NULL REFERENCES app.users(id),
    parent_id INTEGER REFERENCES app.comments(id),
    body      TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE app.audit_log (
    id         BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_id  INTEGER NOT NULL,
    action     TEXT NOT NULL,
    old_data   JSONB,
    new_data   JSONB,
    changed_by INTEGER REFERENCES app.users(id),
    changed_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE analytics.page_views (
    id         BIGSERIAL PRIMARY KEY,
    path       TEXT NOT NULL,
    user_id    INTEGER,
    referrer   TEXT,
    user_agent TEXT,
    viewed_at  TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE analytics.daily_stats (
    stat_date   DATE NOT NULL,
    total_views INTEGER DEFAULT 0,
    total_users INTEGER DEFAULT 0,
    total_posts INTEGER DEFAULT 0,
    PRIMARY KEY (stat_date)
);

-- =============================================================================
-- SEQUENCES (beyond the implicit SERIAL ones)
-- =============================================================================
CREATE SEQUENCE app.invoice_number_seq
    START WITH 1000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 10;

-- =============================================================================
-- FUNCTIONS
-- =============================================================================
CREATE OR REPLACE FUNCTION app.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.audit_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO app.audit_log (table_name, record_id, action, new_data, changed_by)
        VALUES (TG_TABLE_NAME, NEW.id, 'INSERT', to_jsonb(NEW), NEW.id);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO app.audit_log (table_name, record_id, action, old_data, new_data, changed_by)
        VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW), NEW.id);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO app.audit_log (table_name, record_id, action, old_data)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.slugify(input TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN lower(regexp_replace(regexp_replace(input, '[^a-zA-Z0-9\s-]', '', 'g'), '\s+', '-', 'g'));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION app.get_post_comment_count(p_post_id INTEGER)
RETURNS INTEGER AS $$
    SELECT count(*)::integer FROM app.comments WHERE post_id = p_post_id;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION analytics.increment_view(p_path TEXT, p_user_id INTEGER DEFAULT NULL)
RETURNS void AS $$
BEGIN
    INSERT INTO analytics.page_views (path, user_id) VALUES (p_path, p_user_id);
    INSERT INTO analytics.daily_stats (stat_date, total_views)
    VALUES (current_date, 1)
    ON CONFLICT (stat_date) DO UPDATE SET total_views = analytics.daily_stats.total_views + 1;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VIEWS
-- =============================================================================
CREATE VIEW app.published_posts AS
    SELECT p.id, p.title, p.slug, p.body, p.view_count, p.published_at,
           u.username AS author, c.name AS category
    FROM app.posts p
    JOIN app.users u ON u.id = p.author_id
    LEFT JOIN app.categories c ON c.id = p.category_id
    WHERE p.is_draft = false AND p.published_at IS NOT NULL;

CREATE VIEW app.user_stats AS
    SELECT u.id, u.username,
           count(DISTINCT p.id) AS post_count,
           count(DISTINCT cm.id) AS comment_count,
           coalesce(sum(p.view_count), 0) AS total_views
    FROM app.users u
    LEFT JOIN app.posts p ON p.author_id = u.id
    LEFT JOIN app.comments cm ON cm.user_id = u.id
    GROUP BY u.id, u.username;

-- =============================================================================
-- MATERIALIZED VIEWS
-- =============================================================================
CREATE MATERIALIZED VIEW analytics.top_posts AS
    SELECT p.id, p.title, p.slug, u.username AS author,
           p.view_count, count(c.id) AS comment_count,
           p.published_at
    FROM app.posts p
    JOIN app.users u ON u.id = p.author_id
    LEFT JOIN app.comments c ON c.post_id = p.id
    WHERE p.is_draft = false
    GROUP BY p.id, p.title, p.slug, u.username, p.view_count, p.published_at
    ORDER BY p.view_count DESC
WITH DATA;

-- =============================================================================
-- INDEXES
-- =============================================================================
CREATE INDEX idx_users_username ON app.users (username);
CREATE INDEX idx_users_email ON app.users (email);
CREATE INDEX idx_users_status ON app.users (status);
CREATE INDEX idx_users_username_trgm ON app.users USING gin (username gin_trgm_ops);

CREATE UNIQUE INDEX idx_categories_slug ON app.categories (slug);
CREATE INDEX idx_categories_parent ON app.categories (parent_id);

CREATE UNIQUE INDEX idx_posts_slug ON app.posts (slug);
CREATE INDEX idx_posts_author ON app.posts (author_id);
CREATE INDEX idx_posts_category ON app.posts (category_id);
CREATE INDEX idx_posts_published ON app.posts (published_at) WHERE is_draft = false;
CREATE INDEX idx_posts_body_search ON app.posts USING gin (to_tsvector('english', body));

CREATE INDEX idx_comments_post ON app.comments (post_id);
CREATE INDEX idx_comments_user ON app.comments (user_id);
CREATE INDEX idx_comments_parent ON app.comments (parent_id);

CREATE INDEX idx_audit_table_record ON app.audit_log (table_name, record_id);
CREATE INDEX idx_audit_changed_at ON app.audit_log (changed_at);

CREATE INDEX idx_page_views_path ON analytics.page_views (path);
CREATE INDEX idx_page_views_time ON analytics.page_views (viewed_at);

CREATE UNIQUE INDEX idx_top_posts_id ON analytics.top_posts (id);

-- =============================================================================
-- TRIGGERS
-- =============================================================================
CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON app.users
    FOR EACH ROW EXECUTE FUNCTION app.update_updated_at();

CREATE TRIGGER trg_posts_updated_at
    BEFORE UPDATE ON app.posts
    FOR EACH ROW EXECUTE FUNCTION app.update_updated_at();

CREATE TRIGGER trg_users_audit
    AFTER INSERT OR UPDATE OR DELETE ON app.users
    FOR EACH ROW EXECUTE FUNCTION app.audit_changes();

-- =============================================================================
-- RULES
-- =============================================================================
CREATE RULE protect_audit_delete AS
    ON DELETE TO app.audit_log
    DO INSTEAD NOTHING;

CREATE RULE protect_audit_update AS
    ON UPDATE TO app.audit_log
    DO INSTEAD NOTHING;

-- =============================================================================
-- POLICIES (Row Level Security)
-- =============================================================================
ALTER TABLE app.posts ENABLE ROW LEVEL SECURITY;

CREATE POLICY posts_select_published ON app.posts
    FOR SELECT
    USING (is_draft = false OR author_id = current_setting('app.current_user_id', true)::integer);

CREATE POLICY posts_modify_own ON app.posts
    FOR ALL
    USING (author_id = current_setting('app.current_user_id', true)::integer)
    WITH CHECK (author_id = current_setting('app.current_user_id', true)::integer);

-- =============================================================================
-- TEXT SEARCH
-- =============================================================================
CREATE TEXT SEARCH CONFIGURATION app.english_unaccent (COPY = pg_catalog.english);

-- =============================================================================
-- COMMENTS (on objects)
-- =============================================================================
COMMENT ON TABLE app.users IS 'Application users';
COMMENT ON TABLE app.posts IS 'Blog posts written by users';
COMMENT ON COLUMN app.users.score IS 'User reputation score, must be positive';
COMMENT ON FUNCTION app.slugify(TEXT) IS 'Convert text to URL-friendly slug';
COMMENT ON INDEX app.idx_posts_published IS 'Partial index for fast published post lookups';
COMMENT ON VIEW app.published_posts IS 'Published posts with author and category info';

-- =============================================================================
-- DATA
-- =============================================================================

-- Users
INSERT INTO app.users (username, email, status, bio, score) VALUES
    ('alice',   'alice@example.com',   'active',    'Software engineer and blogger',  42),
    ('bob',     'bob@example.com',     'active',    'DevOps enthusiast',              28),
    ('charlie', 'charlie@example.com', 'active',    'Full-stack developer',           35),
    ('diana',   'diana@example.com',   'inactive',  'Data scientist on sabbatical',   19),
    ('eve',     'eve@example.com',     'suspended', NULL,                              5);

-- Categories
INSERT INTO app.categories (name, slug, parent_id, sort_order) VALUES
    ('Technology',   'technology',   NULL, 1),
    ('Programming',  'programming',  1,    1),
    ('Databases',    'databases',    1,    2),
    ('DevOps',       'devops',       1,    3),
    ('Lifestyle',    'lifestyle',    NULL, 2),
    ('Productivity', 'productivity', 5,    1);

-- Tags
INSERT INTO app.tags (name) VALUES
    ('postgresql'), ('go'), ('docker'), ('kubernetes'), ('tutorial'),
    ('beginner'), ('advanced'), ('opinion'), ('howto'), ('announcement');

-- Posts
INSERT INTO app.posts (author_id, category_id, title, slug, body, is_draft, view_count, published_at) VALUES
    (1, 2, 'Getting Started with Go',
     'getting-started-with-go',
     'Go is a statically typed, compiled language designed at Google.',
     false, 1523, now() - interval '30 days'),

    (1, 3, 'PostgreSQL Change Data Capture with pgstream',
     'postgresql-cdc-pgstream',
     'pgstream is a powerful CDC tool for PostgreSQL.',
     false, 2847, now() - interval '14 days'),

    (2, 4, 'Docker Compose for Local Development',
     'docker-compose-local-dev',
     'Docker Compose simplifies multi-container development.',
     false, 956, now() - interval '7 days'),

    (3, 2, 'Understanding Interfaces in Go',
     'understanding-interfaces-go',
     'Interfaces are one of the most powerful features of Go.',
     false, 1102, now() - interval '3 days'),

    (3, 3, 'Advanced PostgreSQL Indexing Strategies',
     'advanced-pg-indexing',
     'Proper indexing is crucial for database performance.',
     true, 0, NULL),

    (1, 6, 'My Productivity Setup in 2025',
     'productivity-setup-2025',
     'After years of experimenting, here is my current productivity stack.',
     false, 445, now() - interval '1 day');

-- Post tags
INSERT INTO app.post_tags (post_id, tag_id) VALUES
    (1, 2), (1, 5), (1, 6),
    (2, 1), (2, 5), (2, 9),
    (3, 3), (3, 4), (3, 5),
    (4, 2), (4, 7),
    (5, 1), (5, 7),
    (6, 8);

-- Comments
INSERT INTO app.comments (post_id, user_id, parent_id, body) VALUES
    (1, 2, NULL, 'Great introduction!'),
    (1, 3, 1,    'Same here.'),
    (1, 4, NULL, 'Could you do a follow-up?'),
    (2, 3, NULL, 'pgstream looks promising.'),
    (2, 1, 4,    'Schema changes are captured via event triggers.'),
    (2, 2, NULL, 'We have been using this in production.'),
    (3, 1, NULL, 'Nice setup!'),
    (4, 1, NULL, 'The implicit interface satisfaction is elegant.'),
    (4, 2, 8,    'Agreed.');

-- Page views
INSERT INTO analytics.page_views (path, user_id, viewed_at) VALUES
    ('/posts/getting-started-with-go',        1, now() - interval '2 days'),
    ('/posts/getting-started-with-go',        2, now() - interval '2 days'),
    ('/posts/getting-started-with-go',        NULL, now() - interval '1 day'),
    ('/posts/postgresql-cdc-pgstream',        3, now() - interval '1 day'),
    ('/posts/postgresql-cdc-pgstream',        NULL, now() - interval '6 hours'),
    ('/posts/docker-compose-local-dev',       2, now() - interval '3 hours'),
    ('/posts/understanding-interfaces-go',    1, now() - interval '1 hour'),
    ('/posts/productivity-setup-2025',        4, now() - interval '30 minutes');

-- Daily stats
INSERT INTO analytics.daily_stats (stat_date, total_views, total_users, total_posts) VALUES
    (current_date - 7,  142, 45, 1),
    (current_date - 6,  198, 62, 0),
    (current_date - 5,  167, 51, 0),
    (current_date - 4,  203, 68, 1),
    (current_date - 3,  189, 59, 0),
    (current_date - 2,  256, 82, 1),
    (current_date - 1,  312, 95, 1),
    (current_date,       87, 31, 0);

-- Set a sequence value
SELECT setval('app.invoice_number_seq', 1042);

-- Refresh the materialized view with the inserted data
REFRESH MATERIALIZED VIEW analytics.top_posts;

COMMIT;
