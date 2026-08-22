#!/usr/bin/env python3
"""Monticello listings collapse to one record per package (soogle issue #1).

A Monticello repository is a flat directory holding every *version* of every
package.  Scrapers that indexed a record per .mcz produced a search result per
version: /package/103936/ and /package/110851/ were both trunk/ToolsTests, one
`ToolsTests-eem.83.mcz` and the other `ToolsTests-fbs.36.mcz`.

Run:  python3 tests/test_monticello.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scrape.monticello import collapse_versions, describe, split_filename

results = []


def check(condition, label):
    results.append(bool(condition))
    print(f"{'PASS' if condition else 'FAIL'}: {label}")


def section(title):
    print(f"\n{title}")
    print("-" * 60)


section("Filenames split into package, author and version")
check(split_filename("ToolsTests-eem.83.mcz") == ("ToolsTests", "eem", 83),
      "a normal versioned package")
check(split_filename("MethodMassage-eem.66.mcz") == ("MethodMassage", "eem", 66),
      "another, from the issue")
# The last hyphen separates the author, so a hyphenated package name survives.
check(split_filename("Multilingual-Encodings-ul.7.mcz")
      == ("Multilingual-Encodings", "ul", 7),
      "a package name containing a hyphen keeps it")
check(split_filename("Kernel-topa.1234.mcm") == ("Kernel", "topa", 1234),
      ".mcm configuration maps split the same way")
check(split_filename("update.squeak.123.mcm") == ("update.squeak.123", None, None),
      "a file with no author.version is its own package, not dropped")
check(split_filename("index.html") == (None, None, None),
      "a non-Monticello file is rejected")
# Found in the real trunk listing: an interrupted upload leaves the fragment
# behind. It is a broken file, not a version, so it must not become a package
# of its own nor be offered as anyone's download.
check(split_filename("HelpSystem-Core-tpr.142.partial.mcz") == (None, None, None),
      "an interrupted upload (.partial.mcz) is rejected")
# Also found in the wild: source.squeak.org/etoys carries
# 'update-bf.31.mcm.mcm', uploaded with its extension doubled. Untreated it
# hides the author and version, so the file becomes a package named after a
# version - the shape this whole change exists to remove.
check(split_filename("update-bf.31.mcm.mcm") == ("update", "bf", 31),
      "a doubled extension is normalised before parsing")
_dbl = collapse_versions(["update-bf.30.mcm", "update-bf.31.mcm.mcm"])
check(list(_dbl) == ["update"] and _dbl["update"]["count"] == 2,
      "so it joins its own package instead of becoming a new one")
check(_dbl["update"]["latest"] == "update-bf.31.mcm.mcm",
      "while the download link keeps the name that exists on disk")
check("HelpSystem-Core" not in collapse_versions(["HelpSystem-Core-tpr.142.partial.mcz"]),
      "and does not create a package on its own")
check(collapse_versions(["A-x.1.mcz", "A-x.2.partial.mcz"])["A"]["latest"] == "A-x.1.mcz",
      "nor displace a real version as the download link")


section("Versions collapse to one record per package - issue #1")
listing = [
    "ToolsTests-eem.83.mcz",
    "ToolsTests-fbs.36.mcz",
    "ToolsTests-ul.34.mcz",
    "MethodMassage-eem.66.mcz",
]
packages = collapse_versions(listing)

check(set(packages) == {"ToolsTests", "MethodMassage"},
      f"4 files become 2 packages (got {sorted(packages)})")
check(packages["ToolsTests"]["count"] == 3,
      "ToolsTests knows it has 3 versions")
check(packages["ToolsTests"]["latest"] == "ToolsTests-eem.83.mcz",
      "and the highest-numbered version represents it")
check(packages["ToolsTests"]["version"] == 83 and packages["ToolsTests"]["author"] == "eem",
      "with that version's number and author")
check(packages["MethodMassage"]["count"] == 1,
      "a package with one version reports one")


section("The representative does not depend on listing order")
forward = collapse_versions(["A-x.1.mcz", "A-y.9.mcz", "A-z.5.mcz"])
reverse = collapse_versions(["A-z.5.mcz", "A-y.9.mcz", "A-x.1.mcz"])
check(forward["A"]["latest"] == reverse["A"]["latest"] == "A-y.9.mcz",
      "highest version wins from either direction")
check(collapse_versions(["A-x.9.mcz", "A-y.9.mcz"])["A"]["latest"] == "A-x.9.mcz",
      "an equal version keeps the one seen first, so scrapes are stable")


section("Unversioned files never displace a real version")
mixed = collapse_versions(["A.mcz", "A-x.4.mcz"])
check(mixed["A"]["latest"] == "A-x.4.mcz" and mixed["A"]["count"] == 2,
      "a bare A.mcz alongside A-x.4.mcz keeps the versioned one")
reordered = collapse_versions(["A-x.4.mcz", "A.mcz"])
check(reordered["A"]["latest"] == "A-x.4.mcz",
      "and in the other order too")


section("Descriptions describe the package, not the repository")
info = packages["ToolsTests"]
desc = describe("ToolsTests", "trunk", "source.squeak.org", info)
check("ToolsTests" in desc and "trunk" in desc,
      "the package and its repository are named")
check("3 versions" in desc and "ToolsTests-eem.83.mcz" in desc,
      "the version count and latest version are stated")
# The bug being fixed: every trunk package carried the repository's own blurb.
check("Welcome to the Squeak Trunk repository" not in desc,
      "the repository's blurb is not presented as the package's description")
# Dialect detection reads name + description, so the source has to stay legible.
check("squeak" in desc.lower(),
      "the text still identifies the dialect for the processor")
check("1 version." in describe("A", "r", "h", {"latest": "A-x.1.mcz", "count": 1}),
      "one version is not called '1 versions'")


print("\n" + "=" * 60)
failed = results.count(False)
print(f"Results: {results.count(True)} passed, {failed} failed")
sys.exit(1 if failed else 0)
