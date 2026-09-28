"""Soogle scrape CLI.

Usage:
    python -m scrape github [--incremental | --since YYYY-MM-DD]
    python -m scrape web <source>          # squeaksource | smalltalkhub | rosettacode | vskb | all
    python -m scrape discover <engine>     # brave | serpapi | bing | ddg
    python -m scrape youtube [--playlists-only]
    python -m scrape custom <source>       # squeakmap | lukas_renggli | sourceforge | launchpad | squeaktrunk | all
    python -m scrape analyze [--limit N] [--min-urls 2] [--show] [--min-score 50]
    python -m scrape process [--limit N]
    python -m scrape submissions [--limit N]
    python -m scrape block <external_id> [--site github] [--reason '...']
    python -m scrape llm-review [--limit N] [--fetch-only] [--review-only] [--model M] [--scope S]
    python -m scrape video-review [--limit N] [--model MODEL] [--scope S]
    python -m scrape status
"""

import argparse
import logging
import sys
from datetime import datetime

from sqlalchemy import func, select

from . import config, db
from .schema import (
    blocklist,
    package_categories,
    package_classes,
    package_methods,
    packages,
    scrape_jobs,
    scrape_raw,
    sites,
    videos,
)

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
        result = scraper.run(incremental=args.incremental, since=args.since)
    print(f"GitHub: found={result['found']} saved={result['saved']} errors={result['errors']}")
    if result["errors"]:
        sys.exit(1)


def cmd_web(args):
    from .web import run_web_scraper
    with db.connection() as conn:
        result = run_web_scraper(conn, args.source)
    if isinstance(result, dict) and "found" in result:
        print(f"{args.source}: found={result['found']} saved={result['saved']} errors={result['errors']}")
    else:
        crashed = [name for name, r in result.items() if r.get("crashed")]
        for name, r in result.items():
            flag = "  CRASHED" if r.get("crashed") else ""
            print(f"{name}: found={r['found']} saved={r['saved']} errors={r['errors']}{flag}")
        if crashed:
            log.error("web scrapers crashed: %s", ", ".join(crashed))
            sys.exit(1)


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
            print(f"Analyze: analyzed={result['analyzed']} promising={result['promising']} "
                  f"errors={result['errors']}")
            if result["promising"]:
                print("\nPromising domains (run with --show to see details):")
                show_results(conn, min_score=50)
            if result["errors"]:
                sys.exit(1)


def cmd_custom(args):
    from .custom import run_custom_scraper
    with db.connection() as conn:
        result = run_custom_scraper(conn, args.source)
    if isinstance(result, dict) and "found" in result:
        print(f"{args.source}: found={result['found']} saved={result['saved']} errors={result['errors']}")
    else:
        crashed = [name for name, r in result.items() if r.get("crashed")]
        for name, r in result.items():
            flag = "  CRASHED" if r.get("crashed") else ""
            print(f"{name}: found={r['found']} saved={r['saved']} errors={r['errors']}{flag}")
        if crashed:
            log.error("custom scrapers crashed: %s", ", ".join(crashed))
            sys.exit(1)


def cmd_process(args):
    from .processor import process_all, process_batch
    with db.connection() as conn:
        if args.limit:
            result = process_batch(conn, limit=args.limit)
        else:
            result = process_all(conn)
    print(f"Process: processed={result['processed']} errors={result['errors']}")
    if result["errors"]:
        sys.exit(1)


def cmd_submissions(args):
    from .submissions import process_submissions
    with db.connection() as conn:
        result = process_submissions(conn, limit=args.limit)
    print(f"Submissions: pending={result['pending']} added={result['added']} "
          f"rejected={result['rejected']} skipped={result['skipped']} "
          f"errors={result['errors']}")


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
            if result["errors"]:
                sys.exit(1)


def cmd_video_review(args):
    from .llm_review import review_videos
    with db.connection() as conn:
        result = review_videos(conn, limit=args.limit, model=args.model,
                               scope=args.scope,
                               since_id=args.since_id,
                               since_date=args.since_date)
    print(f"Video review: reviewed={result['reviewed']} kept={result['kept']} "
          f"blocked={result['blocked']} errors={result['errors']}")
    if result["errors"]:
        sys.exit(1)


def cmd_block(args):
    with db.connection() as conn:
        result = conn.execute(
            db.insert_ignore(
                blocklist,
                {
                    "external_id": args.external_id,
                    "site_name": args.site,
                    "reason": args.reason,
                },
                ["external_id", "site_name"],
            )
        )
        if result.rowcount:
            conn.commit()
            print(f"Blocked: {args.site}/{args.external_id}")
        else:
            print(f"Already blocked: {args.site}/{args.external_id}")

        # Also delete the package if it exists
        row = conn.execute(
            select(packages.c.id)
            .select_from(packages.join(sites, sites.c.id == packages.c.site_id))
            .where(sites.c.name == args.site, packages.c.external_id == args.external_id)
        ).mappings().fetchone()
        if row:
            pkg_id = row["id"]
            conn.execute(
                scrape_raw.update()
                .where(scrape_raw.c.package_id == pkg_id)
                .values(package_id=None)
            )
            conn.execute(package_methods.delete().where(package_methods.c.package_id == pkg_id))
            conn.execute(package_classes.delete().where(package_classes.c.package_id == pkg_id))
            conn.execute(package_categories.delete().where(package_categories.c.package_id == pkg_id))
            conn.execute(packages.delete().where(packages.c.id == pkg_id))
            conn.commit()
            print(f"Deleted package id={pkg_id}")


def cmd_status(args):
    with db.connection() as conn:
        pkg_count = conn.execute(select(func.count()).select_from(packages)).scalar()

        raw_counts = conn.execute(
            select(scrape_raw.c.status, func.count().label("n"))
            .group_by(scrape_raw.c.status)
            .order_by(scrape_raw.c.status)
        ).mappings().fetchall()

        jobs = conn.execute(
            select(
                sites.c.name, scrape_jobs.c.job_type, scrape_jobs.c.status,
                scrape_jobs.c.items_found, scrape_jobs.c.items_processed,
                scrape_jobs.c.items_failed, scrape_jobs.c.started_at,
                scrape_jobs.c.completed_at,
            )
            .select_from(scrape_jobs.join(sites, sites.c.id == scrape_jobs.c.site_id))
            .order_by(scrape_jobs.c.id.desc())
            .limit(10)
        ).mappings().fetchall()

        dialects = conn.execute(
            select(packages.c.dialect, func.count().label("n"))
            .group_by(packages.c.dialect)
            .order_by(func.count().desc())
        ).mappings().fetchall()

        video_count = conn.execute(select(func.count()).select_from(videos)).scalar()

        video_sources = conn.execute(
            select(videos.c.source, func.count().label("n"))
            .group_by(videos.c.source)
            .order_by(func.count().desc())
        ).mappings().fetchall()

    print(f"\nPackages: {pkg_count}")
    print(f"Videos: {video_count}")
    if video_sources:
        print("\nVideos by source:")
        for row in video_sources:
            print(f"  {row['source']}\t{row['n']}")

    print("\nscrape_raw pipeline:")
    for row in raw_counts:
        print(f"  {row['status']}\t{row['n']}")

    if dialects:
        print("\nPackages by dialect:")
        for row in dialects:
            print(f"  {row['dialect']}\t{row['n']}")

    if jobs:
        print("\nRecent scrape jobs:")
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
    gh_mode = gh.add_mutually_exclusive_group()
    gh_mode.add_argument("--incremental", action="store_true", help="Only repos created in the last 30 days")
    gh_mode.add_argument("--since", type=lambda s: datetime.strptime(s, "%Y-%m-%d"), metavar="YYYY-MM-DD",
                         help="Only repos created on or after this date (recovers a gap longer than 30 days)")

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
                               "sourceforge", "launchpad",
                               "squeaktrunk", "all"],
                      help="Custom scraper to run")

    ana = sub.add_parser("analyze", help="LLM analysis of discovered domains")
    ana.add_argument("--limit", type=int, default=None, help="Max domains to analyze")
    ana.add_argument("--min-urls", type=int, default=2, help="Min discovery hits per domain (default 2)")
    ana.add_argument("--show", action="store_true", help="Show previous analysis results")
    ana.add_argument("--min-score", type=int, default=0, help="Min score to show (with --show)")

    proc = sub.add_parser("process", help="Process scrape_raw into packages")
    proc.add_argument("--limit", type=int, default=None, help="Max rows to process")

    subm = sub.add_parser("submissions",
                          help="Process pending user-submitted URLs")
    subm.add_argument("--limit", type=int, default=None,
                      help="Max submissions to process")

    llm = sub.add_parser("llm-review", help="LLM quality review of packages")
    llm.add_argument("--limit", type=int, default=None, help="Max packages to review")
    llm.add_argument("--fetch-only", action="store_true", help="Only fetch READMEs, skip LLM")
    llm.add_argument("--review-only", action="store_true", help="Skip README fetch, LLM only")
    llm.add_argument("--model", default=config.OPENAI_MODEL,
                     help="Model to use (default: %(default)s)")
    llm.add_argument("--scope", choices=["unreviewed", "upgrade", "all"],
                     default="unreviewed",
                     help="unreviewed=new only, upgrade=re-review items from a lower model, all=everything")
    llm.add_argument("--since-id", type=int, default=None,
                     help="Only review packages with id >= N")
    llm.add_argument("--since-date", default=None,
                     help="Only review packages created at or after this datetime (e.g. 2026-04-05)")

    vr = sub.add_parser("video-review", help="LLM quality review of videos")
    vr.add_argument("--limit", type=int, default=None, help="Max videos to review")
    vr.add_argument("--model", default=config.OPENAI_MODEL,
                    help="Model to use (default: %(default)s)")
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
        "submissions": cmd_submissions,
        "llm-review": cmd_llm_review,
        "video-review": cmd_video_review,
        "block": cmd_block,
        "status": cmd_status,
    }
    handlers[args.command](args)


if __name__ == "__main__":
    main()
