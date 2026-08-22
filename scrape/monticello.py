"""Monticello repository filename handling.

A Monticello HTTP repository is a flat directory of `.mcz` files, one per
*version* of a package, named `PackageName-author.NN.mcz`.  A repository that
keeps its history therefore lists the same package many times over:

    ToolsTests-eem.83.mcz
    ToolsTests-fbs.36.mcz
    ToolsTests-ul.34.mcz

Those are one package, not three.  Scrapers that indexed a record per file
produced a search result per version - reported as soogle issue #1, where
/package/103936/ and /package/110851/ were both `trunk/ToolsTests`.

This module is shared by every scraper that walks such a listing, so the
collapsing is done once and the same way.  It is deliberately free of network
and database code so it can be tested directly: see tests/test_monticello.py.
"""

import re

# PackageName-author.NN.mcz
#
# The package part is greedy so the *last* hyphen separates the author, which
# is what Monticello does and what package names containing hyphens require:
# 'Multilingual-Encodings-ul.7.mcz' is version 7 of 'Multilingual-Encodings'.
# The author part excludes hyphens for the same reason.
_VERSIONED = re.compile(
    r"^(?P<package>.+)-(?P<author>[^-]+)\.(?P<version>\d+)\.(?P<ext>mcz|mcm)$"
)

_BARE = re.compile(r"^(?P<package>.+)\.(?P<ext>mcz|mcm)$")

# An upload that was interrupted leaves the part it managed to write behind,
# named '<version>.partial.mcz'. source.squeak.org/trunk has exactly one, out
# of 16934 files: 'HelpSystem-Core-tpr.142.partial.mcz'. It is not a version -
# it is a broken file - so it must neither become a package of its own nor be
# offered as a package's download.
_PARTIAL = re.compile(r"\.partial\.(mcz|mcm)$")

# An upload whose extension got doubled: source.squeak.org/etoys carries
# 'update-bf.31.mcm.mcm'.  Left alone the version-and-author part is hidden
# behind the extra suffix, so the file becomes a package of its own named
# after a version - the very shape issue #1 is about.  The inner extension is
# the real one; strip the repeats before parsing, but keep the name on disk
# for the URL.
_DOUBLED_EXT = re.compile(r"^(?P<base>.+\.(?:mcz|mcm))\.(?:mcz|mcm)$")


def _normalize(filename):
    """Collapse a doubled .mcz/.mcm suffix down to one."""
    while True:
        m = _DOUBLED_EXT.match(filename)
        if not m:
            return filename
        filename = m.group("base")


def split_filename(filename):
    """Split a Monticello filename into (package, author, version).

    author and version are None for a file that does not carry them, which
    real repositories do contain - so those are kept as their own package
    rather than dropped.  A rejected file returns all-None.

        'ToolsTests-eem.83.mcz'        -> ('ToolsTests', 'eem', 83)
        'Multilingual-Encodings-ul.7.mcz'
                                       -> ('Multilingual-Encodings', 'ul', 7)
        'update.squeak.123.mcm'        -> ('update.squeak.123', None, None)
        'update-bf.31.mcm.mcm'         -> ('update', 'bf', 31)
        'X-tpr.142.partial.mcz'        -> (None, None, None)
        'notmonticello.txt'            -> (None, None, None)
    """
    if _PARTIAL.search(filename):
        return None, None, None

    filename = _normalize(filename)

    m = _VERSIONED.match(filename)
    if m:
        return m.group("package"), m.group("author"), int(m.group("version"))

    m = _BARE.match(filename)
    if m:
        return m.group("package"), None, None

    return None, None, None


def collapse_versions(filenames):
    """Group a repository listing by package.

    Returns {package: {'latest', 'author', 'version', 'count'}} where 'latest'
    is the filename of the highest-numbered version - the one worth linking to
    and the only one that should become a search result.

    Ordering is the listing's own, so a scrape stays stable run to run.
    Unversioned files sort below versioned ones, so a stray file never
    displaces a real version as the representative.
    """
    packages = {}
    for filename in filenames:
        package, author, version = split_filename(filename)
        if package is None:
            continue

        entry = packages.get(package)
        if entry is None:
            packages[package] = {
                "latest": filename,
                "author": author,
                "version": version,
                "count": 1,
            }
            continue

        entry["count"] += 1
        # None sorts below every real version; among real versions the highest
        # wins, and an equal number keeps the one already seen so the result
        # does not depend on listing order.
        if version is not None and (entry["version"] is None or version > entry["version"]):
            entry["latest"] = filename
            entry["author"] = author
            entry["version"] = version

    return packages


def describe(package, repository, host, info):
    """A one-line description that is true of the package itself.

    Monticello has no per-package description: a repository has one, and each
    version has a commit message.  Copying the repository's text onto every
    package - which is what produced the identical 'Welcome to the Squeak
    Trunk repository!' blurb on every trunk result in issue #1 - states
    something about the package that is not true of it.

    So this states only what the listing actually tells us.  The repository's
    own description is kept in the scraped metadata under
    'repository_description' rather than being attributed to the package.
    """
    latest = info.get("latest") or ""
    count = info.get("count") or 0
    versions = "1 version" if count == 1 else f"{count} versions"
    return (
        f"Monticello package '{package}' in the {repository} repository "
        f"at {host}. Latest {latest}; {versions}."
    )
