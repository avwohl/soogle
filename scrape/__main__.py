"""Soogle scrape CLI.

Usage:
    python -m scrape github [--incremental]
    python -m scrape web <source>          # squeaksource | smalltalkhub | rosettacode | vskb | all
    python -m scrape discover <engine>     # brave | serpapi | bing | ddg
    python -m scrape youtube [--playlists-only]
    python -m scrape custom <source>       # squeakmap | lukas_renggli | sourceforge | launchpad | all
    python -m scrape analyze [--limit N] [--min-urls 2] [--show] [--min-score 50]
    python -m scrape process [--limit N]
    python -m scrape block <external_id> [--site github] [--reason '...']
    python -m scrape llm-review [--limit N] [--fetch-only] [--review-only] [--model M] [--scope S]
    python -m scrape video-review [--limit N] [--model MODEL] [--scope S]
    python -m scrape status
"""

import sys
import argparse
import logging
from . import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scrape")


def cmd_github(args):
    from .github import GitHubScraper
    with db.connection() as conn:
        scraper = GitHubScraper(conn)
        result = scraper.run(incremental=args.incremental)
    print(f"GitHub: found={result['found']} saved={result['saved']} errors={result['errors']}")


def cmd_web(args):
    from .web import run_web_scraper
    with db.connection() as conn:
        result = run_web_scraper(conn, args.source)
    if isinstance(result, dict) and "found" in result:
        print(f"{args.source}: found={result['found']} saved={result['saved']} errors={result['errors']}")
    else:
        for name, r in result.items():
            print(f"{name}: found={r['found']} saved={r['saved']} errors={r['errors']}")


def cmd_discover(args):
    if args.engine == "youtube":
        from .youtube import YouTubeScraper
        with db.connection() as conn:
            scraper = YouTubeScraper(conn)
            result = scraper.run(playlists_only=False)
    else:
        from .web import DiscoveryScraper
        with db.connection() as conn:
            scraper = DiscoveryScraper(conn)
            result = scraper.run(engine=args.engine, video_only=args.video_only)
    print(f"discover ({args.engine}): found={result['found']} saved={result['saved']} errors={result['errors']}")


def cmd_youtube(args):
    from .youtube import YouTubeScraper
    with db.connection() as conn:
        scraper = YouTubeScraper(conn)
        result = scraper.run(playlists_only=args.playlists_only)
    print(f"youtube: found={result['found']} saved={result['saved']} errors={result['errors']}")


def cmd_analyze(args):
    from .analyze import analyze_domains, show_results
    with db.connection() as conn:
        if args.show:
            show_results(conn, min_score=args.min_score)
        else:
            result = analyze_domains(conn, limit=args.limit, min_urls=args.min_urls)
            print(f"Analyze: analyzed={result['analyzed']} promising={result['promising']}")
            if result["promising"]:
                print("\nPromising domains (run with --show to see details):")
                show_results(conn, min_score=50)


def cmd_custom(args):
    from .custom import run_custom_scraper
    with db.connection() as conn:
        result = run_custom_scraper(conn, args.source)
    if isinstance(result, dict) and "found" in result:
        print(f"{args.source}: found={result['found']} saved={result['saved']} errors={result['errors']}")
    else:
        for name, r in result.items():
            print(f"{name}: found={r['found']} saved={r['saved']} errors={r['errors']}")


def cmd_process(args):
    from .processor import process_all, process_batch
    with db.connection() as conn:
        if args.limit:
            result = process_batch(conn, limit=args.limit)
        else:
            result = process_all(conn)
    print(f"Process: processed={result['processed']} errors={result['errors']}")


def cmd_llm_review(args):
    from .llm_review import fetch_readmes, review_packages
    with db.connection() as conn:
        if not args.review_only:
            fetched = fetch_readmes(conn, limit=args.limit)
            print(f"README fetch: {fetched} fetched")
        if not args.fetch_only:
            result = review_packages(conn, limit=args.limit, model=args.model,
                                     scope=args.scope,
                                     since_id=args.since_id,
                                     since_date=args.since_date)
            print(f"LLM review: reviewed={result['reviewed']} kept={result['kept']} "
                  f"blocked={result['blocked']} errors={result['errors']}")


def cmd_video_review(args):
    from .llm_review import review_videos
    with db.connection() as conn:
        result = review_videos(conn, limit=args.limit, model=args.model,
                               scope=args.scope,
                               since_id=args.since_id,
                               since_date=args.since_date)
    print(f"Video review: reviewed={result['reviewed']} kept={result['kept']} "
          f"blocked={result['blocked']} errors={result['errors']}")


def cmd_block(args):
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT IGNORE INTO blocklist (external_id, site_name, reason) "
                "VALUES (%s, %s, %s)",
                (args.external_id, args.site, args.reason),
            )
            if cur.rowcount:
                conn.commit()
                print(f"Blocked: {args.site}/{args.external_id}")
            else:
                print(f"Already blocked: {args.site}/{args.external_id}")

            # Also delete the package if it exists
            cur.execute(
                "SELECT p.id FROM packages p JOIN sites s ON p.site_id = s.id "
                "WHERE s.name = %s AND p.external_id = %s",
                (args.site, args.external_id),
            )
            row = cur.fetchone()
            if row:
                pkg_id = row["id"]
                cur.execute("UPDATE scrape_raw SET package_id=NULL WHERE package_id=%s", (pkg_id,))
                cur.execute("DELETE FROM package_methods WHERE package_id=%s", (pkg_id,))
                cur.execute("DELETE FROM package_classes WHERE package_id=%s", (pkg_id,))
                cur.execute("DELETE FROM package_categories WHERE package_id=%s", (pkg_id,))
                cur.execute("DELETE FROM packages WHERE id=%s", (pkg_id,))
                conn.commit()
                print(f"Deleted package id={pkg_id}")


def cmd_status(args):
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM packages")
            pkg_count = cur.fetchone()["n"]

            cur.execute(
                "SELECT status, COUNT(*) AS n FROM scrape_raw GROUP BY status ORDER BY status"
            )
            raw_counts = cur.fetchall()

            cur.execute(
                "SELECT s.name, j.job_type, j.status, j.items_found, j.items_processed, "
                "j.items_failed, j.started_at, j.completed_at "
                "FROM scrape_jobs j JOIN sites s ON s.id = j.site_id "
                "ORDER BY j.id DESC LIMIT 10"
            )
            jobs = cur.fetchall()

            cur.execute(
                "SELECT dialect, COUNT(*) AS n FROM packages GROUP BY dialect ORDER BY n DESC"
            )
            dialects = cur.fetchall()

            cur.execute("SELECT COUNT(*) AS n FROM videos")
            video_count = cur.fetchone()["n"]

            cur.execute(
                "SELECT source, COUNT(*) AS n FROM videos GROUP BY source ORDER BY n DESC"
            )
            video_sources = cur.fetchall()

    print(f"\nPackages: {pkg_count}")
    print(f"Videos: {video_count}")
    if video_sources:
        print(f"\nVideos by source:")
        for row in video_sources:
            print(f"  {row['source']}\t{row['n']}")

    print(f"\nscrape_raw pipeline:")
    for row in raw_counts:
        print(f"  {row['status']}\t{row['n']}")

    if dialects:
        print(f"\nPackages by dialect:")
        for row in dialects:
            print(f"  {row['dialect']}\t{row['n']}")

    if jobs:
        print(f"\nRecent scrape jobs:")
        for j in jobs:
            print(
                f"  {j['name']}\t{j['job_type']}\t{j['status']}\t"
                f"found={j['items_found']}\tprocessed={j['items_processed']}\t"
                f"failed={j['items_failed']}"
            )


def main():
    parser = argparse.ArgumentParser(prog="scrape", description="Soogle scraper CLI")
    sub = parser.add_subparsers(dest="command")

    gh = sub.add_parser("github", help="Scrape GitHub Smalltalk repos")
    gh.add_argument("--incremental", action="store_true", help="Only repos updated in last 30 days")

    web = sub.add_parser("web", help="Scrape web sources")
    web.add_argument("source", choices=["squeaksource", "smalltalkhub", "rosettacode", "vskb", "all"],
                     help="Web source to scrape")

    disc = sub.add_parser("discover", help="Discover Smalltalk code via web search")
    disc.add_argument("engine", choices=["brave", "serpapi", "bing", "ddg", "youtube"],
                      help="Search engine to use")
    disc.add_argument("--video-only", action="store_true",
                      help="Only run queries containing 'video'")

    yt = sub.add_parser("youtube", help="Scrape YouTube for Smalltalk videos")
    yt.add_argument("--playlists-only", action="store_true",
                    help="Only scrape known playlists (Pharo MOOC etc.)")

    cust = sub.add_parser("custom", help="Run custom scrapers for analyzed sites")
    cust.add_argument("source",
                      choices=["squeakmap", "lukas_renggli",
                               "sourceforge", "launchpad", "all"],
                      help="Custom scraper to run")

    ana = sub.add_parser("analyze", help="LLM analysis of discovered domains")
    ana.add_argument("--limit", type=int, default=None, help="Max domains to analyze")
    ana.add_argument("--min-urls", type=int, default=2, help="Min discovery hits per domain (default 2)")
    ana.add_argument("--show", action="store_true", help="Show previous analysis results")
    ana.add_argument("--min-score", type=int, default=0, help="Min score to show (with --show)")

    proc = sub.add_parser("process", help="Process scrape_raw into packages")
    proc.add_argument("--limit", type=int, default=None, help="Max rows to process")

    llm = sub.add_parser("llm-review", help="LLM quality review of packages")
    llm.add_argument("--limit", type=int, default=None, help="Max packages to review")
    llm.add_argument("--fetch-only", action="store_true", help="Only fetch READMEs, skip LLM")
    llm.add_argument("--review-only", action="store_true", help="Skip README fetch, LLM only")
    llm.add_argument("--model", default="claude-haiku-4-5-20251001",
                     help="Anthropic model to use (default: claude-haiku-4-5-20251001)")
    llm.add_argument("--scope", choices=["unreviewed", "upgrade", "all"],
                     default="unreviewed",
                     help="unreviewed=new only, upgrade=re-review items from a lower model, all=everything")
    llm.add_argument("--since-id", type=int, default=None,
                     help="Only review packages with id >= N")
    llm.add_argument("--since-date", default=None,
                     help="Only review packages created at or after this datetime (e.g. 2026-04-05)")

    vr = sub.add_parser("video-review", help="LLM quality review of videos")
    vr.add_argument("--limit", type=int, default=None, help="Max videos to review")
    vr.add_argument("--model", default="claude-haiku-4-5-20251001",
                    help="Anthropic model to use (default: claude-haiku-4-5-20251001)")
    vr.add_argument("--scope", choices=["unreviewed", "upgrade", "all"],
                    default="unreviewed",
                    help="unreviewed=new only, upgrade=re-review items from a lower model, all=everything")
    vr.add_argument("--since-id", type=int, default=None,
                     help="Only review videos with id >= N")
    vr.add_argument("--since-date", default=None,
                     help="Only review videos created at or after this datetime (e.g. 2026-04-05)")

    blk = sub.add_parser("block", help="Add a repo to the blocklist and delete it")
    blk.add_argument("external_id", help="e.g. owner/repo for GitHub")
    blk.add_argument("--site", default="github", help="Site name (default: github)")
    blk.add_argument("--reason", default="", help="Why it's blocked")

    sub.add_parser("status", help="Show scrape pipeline status")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    handlers = {
        "github": cmd_github,
        "web": cmd_web,
        "discover": cmd_discover,
        "youtube": cmd_youtube,
        "custom": cmd_custom,
        "analyze": cmd_analyze,
        "process": cmd_process,
        "llm-review": cmd_llm_review,
        "video-review": cmd_video_review,
        "block": cmd_block,
        "status": cmd_status,
    }
    handlers[args.command](args)


if __name__ == "__main__":
    main()
