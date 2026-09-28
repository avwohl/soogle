-- Soogle: Smalltalk Code Search Engine
-- SQLite schema (alternative to db/schema.sql for MySQL).
--
-- Apply with:  sqlite3 soogle.db < db/schema.sqlite.sql
-- (or:  python -c "import sqlite3; sqlite3.connect('soogle.db').executescript(open('db/schema.sqlite.sql').read())")
--
-- Data flow is identical to the MySQL schema: crawlers write scrape_raw,
-- the processor upserts into packages + related tables, scrape_raw is
-- marked processed.  Timestamps are stored as 'YYYY-MM-DD HH:MM:SS' text
-- (what NOW()/datetime('now') produce), JSON columns as TEXT.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- Sites: platforms that host Smalltalk code
-- ---------------------------------------------------------------------------
CREATE TABLE sites (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,          -- 'github', 'smalltalkhub', 'squeaksource'
    display_name    TEXT NOT NULL,
    base_url        TEXT NOT NULL,
    site_type       TEXT NOT NULL,                 -- 'git_host','archive','catalog','web'
    scrape_method   TEXT NOT NULL,                 -- 'github_api', 'http_crawl', 'mcz_download'
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ---------------------------------------------------------------------------
-- Scrape jobs: tracks each crawl run
-- ---------------------------------------------------------------------------
CREATE TABLE scrape_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id         INTEGER NOT NULL,
    job_type        TEXT NOT NULL,                 -- 'full_crawl','incremental','discovery',...
    status          TEXT NOT NULL DEFAULT 'queued',
    started_at      TEXT,
    completed_at    TEXT,
    items_found     INTEGER NOT NULL DEFAULT 0,
    items_processed INTEGER NOT NULL DEFAULT 0,
    items_failed    INTEGER NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),

    FOREIGN KEY (site_id) REFERENCES sites(id)
);
CREATE INDEX idx_site_status ON scrape_jobs(site_id, status);

-- ---------------------------------------------------------------------------
-- Scrape raw: staging table where crawlers land data
-- ---------------------------------------------------------------------------
CREATE TABLE scrape_raw (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_job_id   INTEGER NOT NULL,
    site_id         INTEGER NOT NULL,
    external_id     TEXT NOT NULL,                 -- 'owner/repo', project name, URL
    raw_metadata    TEXT NOT NULL,                 -- JSON payload
    raw_checksum    TEXT NOT NULL,                 -- SHA-256 for change detection
    status          TEXT NOT NULL DEFAULT 'pending',
    error_message   TEXT,
    package_id      INTEGER,                       -- set after processing
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    processed_at    TEXT,

    FOREIGN KEY (scrape_job_id) REFERENCES scrape_jobs(id),
    FOREIGN KEY (site_id) REFERENCES sites(id),
    FOREIGN KEY (package_id) REFERENCES packages(id)
);
CREATE INDEX idx_status ON scrape_raw(status);
CREATE INDEX idx_site_external ON scrape_raw(site_id, external_id);
CREATE INDEX idx_job_status ON scrape_raw(scrape_job_id, status);

-- ---------------------------------------------------------------------------
-- Categories: functional taxonomy for packages
-- ---------------------------------------------------------------------------
CREATE TABLE categories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,          -- 'web', 'database', 'ui_graphics'
    display_name    TEXT NOT NULL,
    description     TEXT,
    sort_order      INTEGER NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- Packages: the core table — one row per unique Smalltalk package
-- ---------------------------------------------------------------------------
CREATE TABLE packages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identity
    name                TEXT NOT NULL,
    qualified_name      TEXT,
    description         TEXT,

    -- Smalltalk-specific
    dialect             TEXT NOT NULL DEFAULT 'unknown',
    dialect_confidence  INTEGER NOT NULL DEFAULT 0,
    file_format         TEXT NOT NULL DEFAULT 'unknown',

    -- Source location
    site_id             INTEGER NOT NULL,
    external_id         TEXT NOT NULL,
    url                 TEXT,
    clone_url           TEXT,

    -- Repo metadata (primarily from GitHub)
    stars               INTEGER NOT NULL DEFAULT 0,
    forks               INTEGER NOT NULL DEFAULT 0,
    size_kb             INTEGER NOT NULL DEFAULT 0,
    license             TEXT,
    is_fork             INTEGER NOT NULL DEFAULT 0,
    is_archived         INTEGER NOT NULL DEFAULT 0,
    default_branch      TEXT,
    topics              TEXT,                      -- JSON array

    -- Dates from the source
    source_created_at   TEXT,
    source_updated_at   TEXT,
    source_pushed_at    TEXT,
    is_active           INTEGER NOT NULL DEFAULT 1,

    -- Display
    readme_excerpt      TEXT,

    -- Deduplication
    content_hash        TEXT,
    canonical_id        INTEGER,

    -- Scrape tracking
    last_scraped_at     TEXT,
    scrape_checksum     TEXT,
    llm_review          TEXT,                              -- model that reviewed, NULL = unreviewed

    -- Row timestamps
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),

    FOREIGN KEY (site_id) REFERENCES sites(id),
    FOREIGN KEY (canonical_id) REFERENCES packages(id),

    -- Uniqueness: one entry per source per external id
    UNIQUE (site_id, external_id)
);
CREATE INDEX idx_dialect ON packages(dialect);
CREATE INDEX idx_stars ON packages(stars);
CREATE INDEX idx_pushed ON packages(source_pushed_at);
CREATE INDEX idx_created ON packages(source_created_at);
CREATE INDEX idx_active ON packages(is_active);
CREATE INDEX idx_format ON packages(file_format);
CREATE INDEX idx_canonical ON packages(canonical_id);
CREATE INDEX idx_name ON packages(name);
CREATE INDEX idx_license ON packages(license);
CREATE INDEX idx_fork ON packages(is_fork);
CREATE INDEX idx_archived ON packages(is_archived);

-- ---------------------------------------------------------------------------
-- Package categories: many-to-many
-- ---------------------------------------------------------------------------
CREATE TABLE package_categories (
    package_id      INTEGER NOT NULL,
    category_id     INTEGER NOT NULL,
    confidence      INTEGER NOT NULL DEFAULT 0,
    is_manual       INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (package_id, category_id),
    FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);
CREATE INDEX idx_category ON package_categories(category_id);

-- ---------------------------------------------------------------------------
-- Package classes: Smalltalk classes found in each package
-- ---------------------------------------------------------------------------
CREATE TABLE package_classes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id      INTEGER NOT NULL,
    class_name      TEXT NOT NULL,
    superclass_name TEXT,
    category        TEXT,
    is_trait        INTEGER NOT NULL DEFAULT 0,

    FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE
);
CREATE INDEX idx_package ON package_classes(package_id);
CREATE INDEX idx_class_name ON package_classes(class_name);
CREATE INDEX idx_superclass ON package_classes(superclass_name);

-- ---------------------------------------------------------------------------
-- Package methods: method selectors found in each package
-- ---------------------------------------------------------------------------
CREATE TABLE package_methods (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id      INTEGER NOT NULL,
    class_id        INTEGER NOT NULL,
    selector        TEXT NOT NULL,
    protocol        TEXT,
    is_class_side   INTEGER NOT NULL DEFAULT 0,
    source_code     TEXT,

    FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
    FOREIGN KEY (class_id) REFERENCES package_classes(id) ON DELETE CASCADE
);
CREATE INDEX idx_method_package ON package_methods(package_id);
CREATE INDEX idx_selector ON package_methods(selector);
CREATE INDEX idx_class ON package_methods(class_id);
CREATE INDEX idx_protocol ON package_methods(protocol);

-- ---------------------------------------------------------------------------
-- Site analyses: LLM assessment of discovered domains
-- ---------------------------------------------------------------------------
CREATE TABLE site_analyses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    domain          TEXT NOT NULL UNIQUE,
    urls_found      INTEGER NOT NULL DEFAULT 0,
    sample_urls     TEXT,                          -- JSON
    root_page_title TEXT,
    has_sitemap     INTEGER NOT NULL DEFAULT 0,
    structured_score INTEGER NOT NULL DEFAULT 0,
    recommendation  TEXT,
    llm_model       TEXT,
    analyzed_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ---------------------------------------------------------------------------
-- Videos: Smalltalk tutorial / talk / demo videos
-- ---------------------------------------------------------------------------
CREATE TABLE videos (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT NOT NULL,
    description     TEXT,
    url             TEXT NOT NULL,
    video_id        TEXT NOT NULL UNIQUE,
    channel_name    TEXT,
    channel_url     TEXT,
    thumbnail_url   TEXT,
    duration_seconds INTEGER,
    published_at    TEXT,
    view_count      INTEGER NOT NULL DEFAULT 0,
    dialect         TEXT NOT NULL DEFAULT 'unknown',
    source          TEXT NOT NULL DEFAULT 'youtube',
    llm_review      TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_video_dialect ON videos(dialect);
CREATE INDEX idx_published ON videos(published_at);
CREATE INDEX idx_views ON videos(view_count);
CREATE INDEX idx_source ON videos(source);

-- ---------------------------------------------------------------------------
-- User-submitted site suggestions
-- ---------------------------------------------------------------------------
CREATE TABLE site_submissions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT NOT NULL,
    comment     TEXT NOT NULL DEFAULT '',
    ip_address  TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ---------------------------------------------------------------------------
-- Blocklist: (site_name, external_id) pairs rejected by the LLM review
-- ---------------------------------------------------------------------------
CREATE TABLE blocklist (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL,
    site_name   TEXT NOT NULL,
    reason      TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE (external_id, site_name)
);

-- ---------------------------------------------------------------------------
-- Seed data: sites
-- ---------------------------------------------------------------------------
INSERT INTO sites (name, display_name, base_url, site_type, scrape_method) VALUES
    ('github',       'GitHub',           'https://github.com',              'git_host', 'github_api'),
    ('smalltalkhub', 'SmalltalkHub',     'http://smalltalkhub.com',         'archive',  'http_crawl'),
    ('squeaksource', 'SqueakSource',     'http://squeaksource.com',         'archive',  'http_crawl'),
    ('squeaksource3','SqueakSource3',    'http://ss3.gemstone.com',         'archive',  'http_crawl'),
    ('squeakmap',    'SqueakMap',        'http://map.squeak.org',           'catalog',  'http_crawl'),
    ('gitlab',       'GitLab',           'https://gitlab.com',              'git_host', 'gitlab_api'),
    ('sourceforge',  'SourceForge',      'https://sourceforge.net',         'archive',  'http_crawl'),
    ('rosettacode',  'Rosetta Code',     'https://rosettacode.org',         'web',      'http_crawl'),
    ('vskb',          'VS Knowledge Base','https://vs-kb.archiv.apis.de',    'archive',  'http_crawl'),
    ('web_discovered','Web Discovered',   '',                                'web',      'discovery');

-- ---------------------------------------------------------------------------
-- Seed data: categories (from feasibility study taxonomy)
-- ---------------------------------------------------------------------------
INSERT INTO categories (name, display_name, sort_order) VALUES
    ('web',                 'Web',                   1),
    ('database',            'Database',              2),
    ('ui_graphics',         'UI / Graphics',         3),
    ('testing',             'Testing',               4),
    ('ide_dev_tools',       'IDE / Dev Tools',       5),
    ('networking',          'Networking',             6),
    ('scientific',          'Scientific',            7),
    ('games',               'Games',                 8),
    ('education',           'Education / Howto',     9),
    ('serialization',       'Serialization',        10),
    ('cloud_infra',         'Cloud / Infra',        11),
    ('system_os',           'System / OS',          12),
    ('math',                'Math',                 13),
    ('multimedia',          'Multimedia',           14),
    ('language_extensions', 'Language Extensions',  15),
    ('concurrency',         'Concurrency',          16),
    ('iot_hardware',        'IoT / Hardware',       17),
    ('packaging_vcs',       'Packaging / VCS',      18),
    ('miscellaneous',       'Miscellaneous',        19);