"""SQLAlchemy Core table metadata for the Soogle schema.

Mirrors db/schema.sql (MySQL) / db/schema.sqlite.sql (SQLite).  Using Core
Table objects instead of raw SQL lets the same statements run on both
dialects — no string translation layer needed.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)

metadata = MetaData()

sites = Table(
    "sites", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String(100)),
    Column("display_name", String(200)),
    Column("base_url", String(500)),
    Column("site_type", String(50)),
    Column("scrape_method", String(50)),
    Column("is_active", Boolean),
    Column("created_at", Text),
    Column("updated_at", Text),
)

scrape_jobs = Table(
    "scrape_jobs", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("site_id", Integer),
    Column("job_type", String(50)),
    Column("status", String(20)),
    Column("started_at", Text),
    Column("completed_at", Text),
    Column("items_found", Integer),
    Column("items_processed", Integer),
    Column("items_failed", Integer),
    Column("error_message", Text),
    Column("created_at", Text),
)

scrape_raw = Table(
    "scrape_raw", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("scrape_job_id", BigInteger),
    Column("site_id", Integer),
    Column("external_id", String(500)),
    Column("raw_metadata", Text),
    Column("raw_checksum", String(64)),
    Column("status", String(20)),
    Column("error_message", Text),
    Column("package_id", BigInteger),
    Column("created_at", Text),
    Column("processed_at", Text),
)

categories = Table(
    "categories", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(100)),
    Column("display_name", String(200)),
    Column("description", Text),
    Column("sort_order", Integer),
)

packages = Table(
    "packages", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String(500)),
    Column("qualified_name", String(500)),
    Column("description", Text),
    Column("dialect", String(50)),
    Column("dialect_confidence", Integer),
    Column("file_format", String(50)),
    Column("site_id", Integer),
    Column("external_id", String(500)),
    Column("url", String(1000)),
    Column("clone_url", String(1000)),
    Column("stars", Integer),
    Column("forks", Integer),
    Column("size_kb", Integer),
    Column("license", String(100)),
    Column("is_fork", Boolean),
    Column("is_archived", Boolean),
    Column("default_branch", String(200)),
    Column("topics", Text),
    Column("source_created_at", Text),
    Column("source_updated_at", Text),
    Column("source_pushed_at", Text),
    Column("is_active", Boolean),
    Column("readme_excerpt", Text),
    Column("content_hash", String(64)),
    Column("canonical_id", BigInteger),
    Column("last_scraped_at", Text),
    Column("scrape_checksum", String(64)),
    Column("created_at", Text),
    Column("updated_at", Text),
    Column("llm_review", String(100)),
)

package_categories = Table(
    "package_categories", metadata,
    Column("package_id", BigInteger, primary_key=True),
    Column("category_id", Integer, primary_key=True),
    Column("confidence", Integer),
    Column("is_manual", Boolean),
)

package_classes = Table(
    "package_classes", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("package_id", BigInteger),
    Column("class_name", String(500)),
    Column("superclass_name", String(500)),
    Column("category", String(500)),
    Column("is_trait", Boolean),
)

package_methods = Table(
    "package_methods", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("package_id", BigInteger),
    Column("class_id", BigInteger),
    Column("selector", String(500)),
    Column("protocol", String(500)),
    Column("is_class_side", Boolean),
    Column("source_code", Text),
)

site_analyses = Table(
    "site_analyses", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("domain", String(500)),
    Column("urls_found", Integer),
    Column("sample_urls", Text),
    Column("root_page_title", String(500)),
    Column("has_sitemap", Boolean),
    Column("structured_score", Integer),
    Column("recommendation", Text),
    Column("llm_model", String(100)),
    Column("analyzed_at", Text),
)

videos = Table(
    "videos", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("title", String(500)),
    Column("description", Text),
    Column("url", String(1000)),
    Column("video_id", String(100)),
    Column("channel_name", String(500)),
    Column("channel_url", String(1000)),
    Column("thumbnail_url", String(1000)),
    Column("duration_seconds", Integer),
    Column("published_at", Text),
    Column("view_count", Integer),
    Column("dialect", String(50)),
    Column("source", String(100)),
    Column("llm_review", String(100)),
    Column("created_at", Text),
    Column("updated_at", Text),
)

site_submissions = Table(
    "site_submissions", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("url", String(2000)),
    Column("comment", Text),
    Column("ip_address", String(45)),
    Column("status", String(20)),
    Column("created_at", Text),
)

blocklist = Table(
    "blocklist", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("external_id", String(500)),
    Column("site_name", String(100)),
    Column("reason", Text),
    Column("created_at", Text),
)