"""Soogle MCP server — exposes the same search service as the Django web app."""

import asyncio

from mcp.server.mcpserver import MCPServer
from sqlalchemy import func, or_, select

from scrape.db import engine
from scrape.schema import (
    categories,
    package_categories,
    package_classes,
    package_methods,
    packages,
    sites,
    videos,
)

server = MCPServer("soogle")


def _rows(stmt):
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().fetchall()]


@server.tool()
def search_packages(
    q: str = "",
    dialect: str = "",
    site: str = "",
    category: str = "",
    sort: str = "relevance",
    limit: int = 25,
) -> list[dict]:
    """Search Smalltalk packages.

    Args:
        q: Free-text query matched against name and description.
        dialect: Filter by dialect (pharo, squeak, cuis, gnu_smalltalk, gemstone, ...).
        site: Filter by source site name (github, smalltalkhub, squeaksource, ...).
        category: Filter by category name (web, database, testing, ui_graphics, ...).
        sort: relevance (stars then name), stars, updated, or name.
        limit: Max results (default 25).
    """
    stmt = (
        select(
            packages.c.id,
            packages.c.name,
            packages.c.qualified_name,
            packages.c.description,
            packages.c.dialect,
            packages.c.stars,
            packages.c.forks,
            packages.c.license,
            packages.c.url,
            packages.c.source_pushed_at,
            packages.c.updated_at,
            sites.c.display_name.label("site"),
        )
        .select_from(packages.join(sites, sites.c.id == packages.c.site_id))
    )
    conds = []
    if q:
        like = f"%{q}%"
        conds.append(or_(packages.c.name.like(like), packages.c.description.like(like)))
    if dialect:
        conds.append(packages.c.dialect == dialect)
    if site:
        conds.append(sites.c.name == site)
    if category:
        stmt = stmt.join(package_categories, package_categories.c.package_id == packages.c.id)
        stmt = stmt.join(categories, categories.c.id == package_categories.c.category_id)
        conds.append(categories.c.name == category)
    if conds:
        stmt = stmt.where(*conds)
    if sort == "stars":
        stmt = stmt.order_by(packages.c.stars.desc(), packages.c.source_pushed_at.desc())
    elif sort == "updated":
        stmt = stmt.order_by(packages.c.source_pushed_at.desc())
    elif sort == "name":
        stmt = stmt.order_by(packages.c.name)
    else:
        stmt = stmt.order_by(packages.c.stars.desc(), packages.c.name)
    return _rows(stmt.limit(limit))


@server.tool()
def get_package(package_id: int) -> dict | None:
    """Get a package's full detail: metadata, categories, classes, and method list (without source code).

    Use get_method to fetch a single method's source.
    """
    with engine.connect() as conn:
        pkg = conn.execute(
            select(
                packages.c.id,
                packages.c.name,
                packages.c.qualified_name,
                packages.c.description,
                packages.c.dialect,
                packages.c.dialect_confidence,
                packages.c.file_format,
                packages.c.external_id,
                packages.c.url,
                packages.c.clone_url,
                packages.c.stars,
                packages.c.forks,
                packages.c.size_kb,
                packages.c.license,
                packages.c.is_fork,
                packages.c.is_archived,
                packages.c.default_branch,
                packages.c.topics,
                packages.c.source_created_at,
                packages.c.source_updated_at,
                packages.c.source_pushed_at,
                packages.c.is_active,
                packages.c.readme_excerpt,
                packages.c.updated_at,
                sites.c.display_name.label("site"),
            )
            .select_from(packages.join(sites, sites.c.id == packages.c.site_id))
            .where(packages.c.id == package_id)
        ).mappings().fetchone()
        if not pkg:
            return None
        cats = conn.execute(
            select(categories.c.name, categories.c.display_name, package_categories.c.confidence)
            .select_from(
                package_categories.join(categories, categories.c.id == package_categories.c.category_id)
            )
            .where(package_categories.c.package_id == package_id)
        ).mappings().fetchall()
        classes = conn.execute(
            select(
                package_classes.c.id,
                package_classes.c.class_name,
                package_classes.c.superclass_name,
                package_classes.c.category,
                package_classes.c.is_trait,
            )
            .where(package_classes.c.package_id == package_id)
            .order_by(package_classes.c.class_name)
        ).mappings().fetchall()
        methods = conn.execute(
            select(
                package_methods.c.id,
                package_methods.c.selector,
                package_methods.c.protocol,
                package_methods.c.is_class_side,
                package_classes.c.class_name,
            )
            .select_from(
                package_methods.join(package_classes, package_classes.c.id == package_methods.c.class_id)
            )
            .where(package_methods.c.package_id == package_id)
            .order_by(package_classes.c.class_name, package_methods.c.selector)
        ).mappings().fetchall()
    return {
        "package": dict(pkg),
        "categories": [dict(c) for c in cats],
        "classes": [dict(c) for c in classes],
        "methods": [dict(m) for m in methods],
    }


@server.tool()
def get_method(package_id: int, class_name: str, selector: str) -> dict | None:
    """Get a single method's full source code from a package."""
    with engine.connect() as conn:
        row = conn.execute(
            select(package_methods, package_classes.c.class_name)
            .select_from(
                package_methods.join(package_classes, package_classes.c.id == package_methods.c.class_id)
            )
            .where(
                package_methods.c.package_id == package_id,
                package_classes.c.class_name == class_name,
                package_methods.c.selector == selector,
            )
        ).mappings().fetchone()
    return dict(row) if row else None


@server.tool()
def list_sources() -> list[dict]:
    """List all active source sites with their package counts."""
    return _rows(
        select(
            sites.c.name,
            sites.c.display_name,
            sites.c.base_url,
            sites.c.site_type,
            func.count(packages.c.id).label("package_count"),
        )
        .select_from(sites.outerjoin(packages, packages.c.site_id == sites.c.id))
        .where(sites.c.is_active == True)
        .group_by(sites.c.id)
        .order_by(func.count(packages.c.id).desc())
    )


@server.tool()
def search_videos(
    q: str = "",
    dialect: str = "",
    sort: str = "views",
    limit: int = 24,
) -> list[dict]:
    """Search Smalltalk videos.

    Args:
        q: Free-text query matched against title and description.
        dialect: Filter by dialect (pharo, squeak, cuis, general, ...).
        sort: views, newest, or title.
        limit: Max results (default 24).
    """
    stmt = select(videos)
    conds = []
    if q:
        like = f"%{q}%"
        conds.append(or_(videos.c.title.like(like), videos.c.description.like(like)))
    if dialect:
        conds.append(videos.c.dialect == dialect)
    if conds:
        stmt = stmt.where(*conds)
    if sort == "newest":
        stmt = stmt.order_by(videos.c.published_at.desc())
    elif sort == "title":
        stmt = stmt.order_by(videos.c.title)
    else:
        stmt = stmt.order_by(videos.c.view_count.desc())
    return _rows(stmt.limit(limit))


def main():
    asyncio.run(server.run_stdio_async())


if __name__ == "__main__":
    main()