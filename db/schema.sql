-- Soogle: Smalltalk Code Search Engine
-- Database Schema
--
-- Data flow:
--   1. Crawlers write raw data into scrape_raw (staging table)
--   2. Processor reads pending rows, parses, detects dialect, categorizes
--   3. Processor atomically upserts into packages + related tables in a single transaction
--   4. scrape_raw row marked processed
--
-- Atomic update pattern (per package):
--   BEGIN;
--     INSERT INTO packages ... ON DUPLICATE KEY UPDATE ...;
--     DELETE FROM package_classes WHERE package_id = ?;
--     DELETE FROM package_methods WHERE package_id = ?;
--     DELETE FROM package_categories WHERE package_id = ?;
--     INSERT INTO package_classes ...;
--     INSERT INTO package_methods ...;
--     INSERT INTO package_categories ...;
--     UPDATE scrape_raw SET status='processed', package_id=? WHERE id=?;
--   COMMIT;
--
-- Change detection: scrape_raw.raw_checksum vs packages.scrape_checksum
-- allows skipping unchanged entries without reprocessing.

CREATE DATABASE IF NOT EXISTS soogle
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE soogle;

-- ---------------------------------------------------------------------------
-- Sites: platforms that host Smalltalk code
-- ---------------------------------------------------------------------------
CREATE TABLE sites (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,          -- 'github', 'smalltalkhub', 'squeaksource'
    display_name    VARCHAR(200) NOT NULL,
    base_url        VARCHAR(500) NOT NULL,
    site_type       ENUM('git_host','archive','catalog','web') NOT NULL,
    scrape_method   VARCHAR(50)  NOT NULL,                 -- 'github_api', 'http_crawl', 'mcz_download'
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Scrape jobs: tracks each crawl run
-- ---------------------------------------------------------------------------
CREATE TABLE scrape_jobs (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    site_id         INT UNSIGNED NOT NULL,
    job_type        ENUM('full_crawl','incremental','single_package','discovery') NOT NULL,
    status          ENUM('queued','running','completed','failed') NOT NULL DEFAULT 'queued',
    started_at      TIMESTAMP NULL,
    completed_at    TIMESTAMP NULL,
    items_found     INT UNSIGNED NOT NULL DEFAULT 0,
    items_processed INT UNSIGNED NOT NULL DEFAULT 0,
    items_failed    INT UNSIGNED NOT NULL DEFAULT 0,
    error_message   TEXT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (site_id) REFERENCES sites(id),
    INDEX idx_site_status (site_id, status)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Scrape raw: staging table where crawlers land data
--
-- Crawlers write here, processors read.  Each row is one scraped entity
-- (one repo, one SqueakSource project, one web page, etc.)
-- ---------------------------------------------------------------------------
CREATE TABLE scrape_raw (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    scrape_job_id   BIGINT UNSIGNED NOT NULL,
    site_id         INT UNSIGNED NOT NULL,
    external_id     VARCHAR(500) NOT NULL,                 -- 'owner/repo', project name, URL
    raw_metadata    JSON NOT NULL,                         -- full API response / scraped payload
    raw_checksum    CHAR(64) NOT NULL,                     -- SHA-256 of raw_metadata for change detection
    status          ENUM('pending','processing','processed','failed','skipped') NOT NULL DEFAULT 'pending',
    error_message   TEXT NULL,
    package_id      BIGINT UNSIGNED NULL,                  -- set after processing
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at    TIMESTAMP NULL,

    FOREIGN KEY (scrape_job_id) REFERENCES scrape_jobs(id),
    FOREIGN KEY (site_id) REFERENCES sites(id),
    INDEX idx_status (status),
    INDEX idx_site_external (site_id, external_id),
    INDEX idx_job_status (scrape_job_id, status)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Categories: functional taxonomy for packages
-- ---------------------------------------------------------------------------
CREATE TABLE categories (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,          -- 'web', 'database', 'ui_graphics'
    display_name    VARCHAR(200) NOT NULL,                 -- 'Web', 'Database', 'UI / Graphics'
    description     TEXT NULL,
    sort_order      INT NOT NULL DEFAULT 0
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Packages: the core table — one row per unique Smalltalk package
-- ---------------------------------------------------------------------------
CREATE TABLE packages (
    id                  BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

    -- Identity
    name                VARCHAR(500) NOT NULL,             -- package/repo name
    qualified_name      VARCHAR(500) NULL,                 -- 'owner/repo' for GitHub
    description         TEXT NULL,

    -- Smalltalk-specific
    dialect             ENUM(
                            'pharo','squeak','cuis','gnu_smalltalk','gemstone',
                            'visualworks','dolphin','va_smalltalk','st80','unknown'
                        ) NOT NULL DEFAULT 'unknown',
    dialect_confidence  TINYINT UNSIGNED NOT NULL DEFAULT 0,   -- 0-100
    file_format         ENUM(
                            'chunk_fileout','tonel','cuis_package','monticello',
                            'gnu_star','topaz','dolphin_pax','mixed','unknown'
                        ) NOT NULL DEFAULT 'unknown',

    -- Source location
    site_id             INT UNSIGNED NOT NULL,
    external_id         VARCHAR(500) NOT NULL,
    url                 VARCHAR(1000) NULL,
    clone_url           VARCHAR(1000) NULL,

    -- Repo metadata (primarily from GitHub)
    stars               INT UNSIGNED NOT NULL DEFAULT 0,
    forks               INT UNSIGNED NOT NULL DEFAULT 0,
    size_kb             INT UNSIGNED NOT NULL DEFAULT 0,
    license             VARCHAR(100) NULL,
    is_fork             BOOLEAN NOT NULL DEFAULT FALSE,
    is_archived         BOOLEAN NOT NULL DEFAULT FALSE,
    default_branch      VARCHAR(200) NULL,
    topics              JSON NULL,                         -- ["pharo", "seaside", "web"]

    -- Dates from the source
    source_created_at   TIMESTAMP NULL,
    source_updated_at   TIMESTAMP NULL,
    source_pushed_at    TIMESTAMP NULL,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,

    -- Display
    readme_excerpt      TEXT NULL,

    -- Deduplication
    content_hash        CHAR(64) NULL,                     -- SHA-256 of significant content
    canonical_id        BIGINT UNSIGNED NULL,               -- if duplicate, points to canonical

    -- Scrape tracking
    last_scraped_at     TIMESTAMP NULL,
    scrape_checksum     CHAR(64) NULL,                     -- matches scrape_raw.raw_checksum

    -- Row timestamps
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (site_id) REFERENCES sites(id),
    FOREIGN KEY (canonical_id) REFERENCES packages(id),

    -- Uniqueness: one entry per source per external id
    UNIQUE INDEX ux_site_external (site_id, external_id),

    -- Search / filter indexes
    INDEX idx_dialect (dialect),
    INDEX idx_stars (stars DESC),
    INDEX idx_pushed (source_pushed_at DESC),
    INDEX idx_created (source_created_at),
    INDEX idx_active (is_active),
    INDEX idx_format (file_format),
    INDEX idx_canonical (canonical_id),
    INDEX idx_name (name(200)),
    INDEX idx_license (license),
    INDEX idx_fork (is_fork),
    INDEX idx_archived (is_archived),

    -- Full-text on name + description for MySQL-side search
    FULLTEXT INDEX ftx_name_desc (name, description)
) ENGINE=InnoDB;

-- Add FK from scrape_raw back to packages (after packages exists)
ALTER TABLE scrape_raw
    ADD FOREIGN KEY (package_id) REFERENCES packages(id);

-- ---------------------------------------------------------------------------
-- Package categories: many-to-many
-- ---------------------------------------------------------------------------
CREATE TABLE package_categories (
    package_id      BIGINT UNSIGNED NOT NULL,
    category_id     INT UNSIGNED NOT NULL,
    confidence      TINYINT UNSIGNED NOT NULL DEFAULT 0,   -- 0-100
    is_manual       BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (package_id, category_id),
    FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
    INDEX idx_category (category_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Package classes: Smalltalk classes found in each package
-- Enables "search by class name" and "browse class hierarchy"
-- ---------------------------------------------------------------------------
CREATE TABLE package_classes (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    package_id      BIGINT UNSIGNED NOT NULL,
    class_name      VARCHAR(500) NOT NULL,
    superclass_name VARCHAR(500) NULL,
    category        VARCHAR(500) NULL,                     -- Smalltalk class category string
    is_trait        BOOLEAN NOT NULL DEFAULT FALSE,

    FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
    INDEX idx_package (package_id),
    INDEX idx_class_name (class_name(200)),
    INDEX idx_superclass (superclass_name(200))
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Package methods: method selectors found in each package
-- Enables "implementors of" and "senders of" style searches
-- ---------------------------------------------------------------------------
CREATE TABLE package_methods (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    package_id      BIGINT UNSIGNED NOT NULL,
    class_id        BIGINT UNSIGNED NOT NULL,
    selector        VARCHAR(500) NOT NULL,
    protocol        VARCHAR(500) NULL,                     -- method protocol/category
    is_class_side   BOOLEAN NOT NULL DEFAULT FALSE,
    source_code     TEXT NULL,

    FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
    FOREIGN KEY (class_id) REFERENCES package_classes(id) ON DELETE CASCADE,
    INDEX idx_package (package_id),
    INDEX idx_selector (selector(200)),
    INDEX idx_class (class_id),
    INDEX idx_protocol (protocol(100))
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Site analyses: LLM assessment of discovered domains
-- ---------------------------------------------------------------------------
CREATE TABLE site_analyses (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    domain          VARCHAR(500) NOT NULL,
    urls_found      INT UNSIGNED NOT NULL DEFAULT 0,          -- discovery hits from this domain
    sample_urls     JSON NULL,                                -- representative URLs we found
    root_page_title VARCHAR(500) NULL,
    has_sitemap     BOOLEAN NOT NULL DEFAULT FALSE,
    structured_score TINYINT UNSIGNED NOT NULL DEFAULT 0,     -- 0-100, LLM's confidence it has structured data
    recommendation  TEXT NULL,                                -- LLM analysis text
    llm_model       VARCHAR(100) NULL,
    analyzed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE INDEX ux_domain (domain)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Videos: Smalltalk tutorial / talk / demo videos
-- ---------------------------------------------------------------------------
CREATE TABLE videos (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    title           VARCHAR(500) NOT NULL,
    description     TEXT NULL,
    url             VARCHAR(1000) NOT NULL,                -- watch URL
    video_id        VARCHAR(100) NOT NULL,                 -- YouTube video ID or other platform ID
    channel_name    VARCHAR(500) NULL,
    channel_url     VARCHAR(1000) NULL,
    thumbnail_url   VARCHAR(1000) NULL,
    duration_seconds INT UNSIGNED NULL,
    published_at    TIMESTAMP NULL,
    view_count      INT UNSIGNED NOT NULL DEFAULT 0,
    dialect         ENUM(
                        'pharo','squeak','cuis','gnu_smalltalk','gemstone',
                        'visualworks','dolphin','va_smalltalk','st80','general','unknown'
                    ) NOT NULL DEFAULT 'unknown',
    source          VARCHAR(100) NOT NULL DEFAULT 'youtube',  -- 'youtube', 'mooc_pharo', etc.
    llm_review      VARCHAR(100) NULL,                        -- model that reviewed, NULL = unreviewed
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE INDEX ux_video_id (video_id),
    INDEX idx_dialect (dialect),
    INDEX idx_published (published_at DESC),
    INDEX idx_views (view_count DESC),
    INDEX idx_source (source),
    FULLTEXT INDEX ftx_title_desc (title, description)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- User-submitted site suggestions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS site_submissions (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    url         VARCHAR(2000)   NOT NULL,
    comment     TEXT            NOT NULL DEFAULT '',
    ip_address  VARCHAR(45)     NOT NULL DEFAULT '',
    status      ENUM('pending','reviewed','added','rejected') NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

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
    ('miscellaneous',       'Miscellaneous',        99);
