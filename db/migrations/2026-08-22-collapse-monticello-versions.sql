-- Remove the per-version package rows left by the Monticello scrapers.
--
-- soogle issue #1: SqueakTrunkScraper and LukasRenggliScraper keyed each
-- record on a .mcz filename, so every *version* of every package became its
-- own package row - /package/103936/ and /package/110851/ were both
-- trunk/ToolsTests. They now key on {repository}/{package} and collapse the
-- versions behind it, but the rows already written keep their old identity and
-- will never be updated by a rescrape, because nothing will ever produce those
-- external_ids again.
--
-- The old rows are exactly those whose external_id is a filename. New rows
-- cannot look like that: a package name never ends in .mcz or .mcm.
--
-- Scale, measured against the live listing on 2026-08-22: trunk alone lists
-- 16934 .mcz/.mcm files for 125 packages, so the great majority of what this
-- deletes is duplicates of something the rescrape will recreate correctly.
--
-- Run the SELECT first and look at the number before running the rest.

-- ---------------------------------------------------------------------------
-- Dry run: how many rows, and from where
-- ---------------------------------------------------------------------------
SELECT s.name AS site, COUNT(*) AS stale_packages
  FROM packages p
  JOIN sites s ON s.id = p.site_id
 WHERE s.name IN ('squeaktrunk', 'lukas_renggli')
   AND (p.external_id LIKE '%.mcz' OR p.external_id LIKE '%.mcm')
 GROUP BY s.name;

-- ---------------------------------------------------------------------------
-- The deletion
-- ---------------------------------------------------------------------------
START TRANSACTION;

CREATE TEMPORARY TABLE _stale_pkg (id BIGINT UNSIGNED PRIMARY KEY);

INSERT INTO _stale_pkg (id)
SELECT p.id
  FROM packages p
  JOIN sites s ON s.id = p.site_id
 WHERE s.name IN ('squeaktrunk', 'lukas_renggli')
   AND (p.external_id LIKE '%.mcz' OR p.external_id LIKE '%.mcm');

-- scrape_raw.package_id has no ON DELETE CASCADE, so detach first. The raw
-- rows are kept, marked skipped, rather than deleted: they are the record of
-- what the scraper actually saw, and dropping them would lose that history.
UPDATE scrape_raw
   SET package_id = NULL,
       status = 'skipped',
       error_message = 'superseded: per-version row collapsed (issue #1)'
 WHERE package_id IN (SELECT id FROM _stale_pkg);

-- Deduplication may have pointed a surviving row at one of these as its
-- canonical. Clear those before the rows vanish.
UPDATE packages
   SET canonical_id = NULL
 WHERE canonical_id IN (SELECT id FROM _stale_pkg);

-- package_categories, package_classes and friends cascade, so this is enough.
DELETE FROM packages WHERE id IN (SELECT id FROM _stale_pkg);

DROP TEMPORARY TABLE _stale_pkg;

COMMIT;

-- ---------------------------------------------------------------------------
-- Afterwards
-- ---------------------------------------------------------------------------
-- Rescrape to write the collapsed rows:
--     python -m scrape custom squeaktrunk
--     python -m scrape custom lukas_renggli
--     python -m scrape process
--
-- Expect roughly 125 packages from trunk rather than ~17000, plus the other
-- projects on source.squeak.org.
