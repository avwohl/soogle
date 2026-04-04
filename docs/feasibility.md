# Soogle: A Smalltalk Code Search Engine -- Feasibility Study

Date: 2026-03-31


## 1. Vision

A web-wide search engine for Smalltalk source code. Users can search across
GitHub, legacy Smalltalk repositories, blogs, books, and other web sources for
Smalltalk code -- filtered by dialect, category, age, size, and activity.


## 2. Can the Data Be Found and Scraped?

### 2.1 GitHub (primary source, ~14,200 repos)

GitHub is overwhelmingly the dominant platform for Smalltalk code today.

**API capabilities:**

    Endpoint                    What it searches
    GET /search/repositories    Repos by language, stars, date, size, topics
    GET /search/code            File contents (files under 384 KB, default branch)
    GET /search/commits         Commit messages and metadata

The qualifier `language:Smalltalk` works in both web UI and REST API. GitHub
uses Linguist for language detection; Linguist recognizes `.st` files as
Smalltalk (language ID 326).

**Metadata returned per repo (80+ fields):**

    created_at          When the repo was created
    pushed_at           Last commit date (effectively "last changed")
    updated_at          Last metadata modification
    size                Repo size in KB
    stargazers_count    Stars
    forks_count         Forks
    language            Primary language
    topics              User-applied tags (array)
    description         Repo description
    license             License info
    archived            Whether archived
    fork                Whether it is a fork
    default_branch      e.g. "main"

**Rate limits:**

    Limit                       Authenticated       Unauthenticated
    General REST API            5,000 req/hour      60 req/hour
    Search endpoints            30 req/minute       10 req/minute
    Code search                 ~10 req/minute      Auth required

**Critical limitation:** Each search query returns at most 1,000 results (10
pages x 100 results). To enumerate all ~14,200 Smalltalk repos, queries must be
segmented by date range or size range to keep each segment under 1,000 results.
This is a well-known workaround.


### 2.2 Legacy Smalltalk-Specific Platforms

    Platform          Status                      Est. Projects
    SmalltalkHub      Read-only archive (~2020)    ~1,483
    SqueakSource      Active (read/write)          ~3,634
    SqueakSource3     Active, aging                Unknown
    SqueakMap         Active index/catalog         ~1,500+
    Cincom Store      Active (commercial)          ~2,000+
    VAST Community    Active (GitHub)              12
    GsDevKit          Active (GitHub)              55
    Cuis Smalltalk    Active (GitHub)              33+

SmalltalkHub hosts static .mcz (Monticello) archives -- still downloadable.
SqueakSource serves Monticello repos and can be enumerated. SqueakMap is a
catalog pointing to packages hosted elsewhere. Cincom Store is database-backed
and behind commercial licensing -- difficult to scrape.


### 2.3 Other Code Hosts

    Platform        Smalltalk Presence
    GitLab          ~6 projects tagged "smalltalk"
    SourceForge     47 Smalltalk projects (mostly legacy)
    Bitbucket       Minimal; no Smalltalk language filter

These are negligible compared to GitHub.


### 2.4 Web Sources Beyond Repositories

    Source                      Feasibility    Notes
    Free books/tutorials        High           PDFs with extractable code blocks
    Rosetta Code                High           Structured, labeled by dialect
    Planet Smalltalk (blogs)    Medium         Blog aggregator; code in prose
    Pharo Weekly blog           Medium         Weekly news with code examples
    Mailing list archives       Medium         Code embedded in discussion
    Stack Overflow              Medium         Tagged questions with code
    Commercial dialect code     Low            Behind paywalls


### 2.5 Scraping Feasibility by File Format

    Format             Extension       Scrape Difficulty    Dialects
    Chunk/Fileout      .st, .cs        Easy (plain text)    All
    Tonel              .st in dirs     Easy (plain text)    Pharo, GemStone, VA
    Cuis Package       .pck.st         Easy (plain text)    Cuis
    Monticello         .mcz            Medium (unzip)       Pharo, Squeak
    GNU Smalltalk      .star           Medium (archive)     GNU Smalltalk
    Topaz Fileout      .gs             Easy (plain text)    GemStone
    Dolphin PAX/CLS    .pax, .cls      Easy (text chunks)   Dolphin
    Parcels            .pcl/.pst       Hard (binary)        VisualWorks
    ENVY               .dat            Hard (binary)        VA Smalltalk
    Dolphin PAC        .pac            Hard (binary)        Dolphin


## 3. Can Smalltalk Dialects Be Identified Automatically?

Yes, with reasonable accuracy. Strong signals:

    Signal                                              Dialect
    Tonel .properties file with #format : #tonel        Pharo (post-Pharo 7)
    Class { #name: ... #superclass: ... } syntax        Pharo or GemStone (Tonel)
    .pck.st extension with !provides: header            Cuis
    package.xml with <package> tags                     GNU Smalltalk
    .star archive format                                GNU Smalltalk
    PAX/CLS files referencing "Dolphin"                 Dolphin
    Topaz commands (set class, doit, commit)            GemStone
    #category : 'xxx' (spaces around colon)             Pharo convention
    Heavy Trait usage                                   Pharo
    BaselineOf* / ConfigurationOf* classes              Pharo (some Squeak)
    Timestamp format in methodsFor:stamp:               Squeak vs Pharo

Weak signal: plain `.st` chunk-format fileouts look nearly identical across
Squeak, Pharo, and Cuis. Additional heuristics (class names, category naming
conventions, repository metadata/topics) can disambiguate.

GitHub's `topic:` qualifier provides an additional signal:

    language:Smalltalk topic:pharo     ~1,020 repos
    language:Smalltalk topic:squeak    ~69 repos

These are incomplete (only repos where owners applied topics) but useful.


## 4. Estimated Smalltalk Package Universe

    Source                              Count
    GitHub (language:Smalltalk)         ~14,210 repos
    SqueakSource                        ~3,634 projects
    Cincom Public Store                 ~2,000+ packages
    SqueakMap                           ~1,500+ entries
    SmalltalkHub (archived)             ~1,483 projects
    GitHub "smalltalk" topic            ~926 repos
    Pharo Catalog                       ~483 projects
    GsDevKit (GemStone)                 55 repos
    SourceForge                         47 projects
    Cuis Smalltalk (GitHub)             33+ repos
    VAST Community Hub                  12 repos
    GitLab                              6 projects
    Dolphin Smalltalk (GitHub)          4 repos

    Gross total                         ~24,000+
    Estimated unique (after dedup)      ~15,000 - 18,000

There is heavy overlap: many SmalltalkHub projects migrated to GitHub, SqueakMap
entries often point to SqueakSource, and some Pharo Catalog entries exist on both
SmalltalkHub and GitHub.

By comparison: npm has ~3 million packages, PyPI ~500,000+, RubyGems ~180,000+.
The Smalltalk ecosystem is small -- which makes comprehensive indexing feasible.


## 5. Does Such a Search Already Exist?

### 5.1 No Dedicated Smalltalk Code Search Engine Exists

There is no public web service specifically for searching Smalltalk code
across repositories, dialects, and web sources.

Smalltalk IDEs have rich built-in search (class browsers, "senders of",
"implementors of", Spotter in Pharo, Glamorous Toolkit) but these only
search within a single live image.

### 5.2 General Code Search Engines

    Engine              Language Filter?    Smalltalk?     Notes
    GitHub Code Search  Yes (language:)     Yes            Covers GitHub only
    Sourcegraph         Yes (lang:)         Yes (.st)      SaaS and self-hosted
    grep.app            Yes                 Likely         ~1M+ GitHub repos
    SearchCode          Yes (300+ langs)    Yes            Multi-platform index
    OpenGrok            Yes                 Configurable   Self-hosted, enterprise
    Google              No code filter      No             General web search

GitHub Code Search is the closest existing tool -- but it only covers GitHub,
has no dialect awareness, no Smalltalk-specific categorization, and the 1,000
result limit makes exhaustive browsing impossible. No existing engine provides:

    - Dialect filtering (Pharo / Squeak / Cuis / ST-80 / GNU / etc.)
    - Smalltalk-specific categorization (game, library, scientific, etc.)
    - Cross-platform search (GitHub + SqueakSource + SmalltalkHub + web)
    - Metadata enrichment (age, size, activity, dialect, category)


## 6. Proposed Feature Set

### 6.1 Search Modes

    Mode                Description
    Text / keyword      Full-text search of Smalltalk source code
    Class / method      Search by class name, method selector, or protocol
    Category browse     Browse by functional category
    Dialect filter      Filter by Pharo, Squeak, Cuis, GNU, ST-80, etc.
    Metadata filter     Filter by age, size, last changed, stars, license

### 6.2 Categorization Taxonomy

Drawing from SqueakMap (~32 categories) and the Pharo ecosystem:

    Category              Examples
    Web                   Seaside, Zinc, Teapot, REST clients
    Database              GLORP, Magma, OmniBase, SQLite bindings
    UI / Graphics         Morphic, Spec, Roassal, Bloc
    Testing               SUnit, Mocketry, mutation testing
    IDE / Dev Tools       Refactoring, code browsers, linters
    Networking            HTTP, WebSocket, SMTP, DNS, SSH
    Scientific            PolyMath, statistics, machine learning
    Games                 Retro games, game engines, eToys
    Education / Howto     Tutorials, examples, learning resources
    Serialization         JSON, XML, STON, CSV, MessagePack
    Cloud / Infra         AWS, Docker, CI/CD tooling
    System / OS           FFI, file system, process management
    Math                  Linear algebra, cryptography, numerics
    Multimedia            Sound, image processing, animation
    Language Extensions   Traits, pragmas, compiler extensions
    Concurrency           Actors, promises, parallel collections
    IoT / Hardware        GPIO, embedded, sensors
    Packaging / VCS       Metacello, Monticello, Iceberg, Tonel
    Miscellaneous         Everything else

Auto-categorization via repo description, README, topic tags, class/package
names, and keyword heuristics. Manual curation for high-value packages.

### 6.3 Metadata Fields

    Field               Source
    Dialect             File format analysis + repo topics + heuristics
    Category            Auto-classified + manual tags
    Created             GitHub API created_at / file timestamps
    Last changed        GitHub API pushed_at / last commit date
    Size                Repo size / line count
    Stars / Forks       GitHub API
    License             GitHub API license field
    Active?             Last push within N months
    Description         Repo description / README excerpt


## 7. Data Acquisition Strategy

### Phase 1: GitHub (covers ~80% of visible Smalltalk code)

    1. Enumerate all ~14,200 Smalltalk repos via segmented date-range queries
    2. Fetch metadata for each repo (created, pushed, size, stars, topics, etc.)
    3. Clone or shallow-clone repos to index source code
    4. Run dialect detection on file contents
    5. Auto-categorize via description, topics, README, and class names
    6. Build full-text search index

    Estimated effort:  ~150 segmented API queries to enumerate
                       ~14,200 metadata fetches (within rate limits)
                       ~50-200 GB storage for cloned repos
                       1-2 days for initial full crawl

### Phase 2: Legacy Platforms

    1. Crawl SmalltalkHub static archive (download .mcz files)
    2. Crawl SqueakSource (enumerate and download Monticello packages)
    3. Import SqueakMap catalog as a metadata source
    4. Parse .mcz archives (unzip, extract snapshot/source.st)
    5. Deduplicate against GitHub index (match by name, author, content hash)

### Phase 3: Web Sources

    1. Scrape Rosetta Code Smalltalk examples
    2. Index free Smalltalk books/tutorials (extract code blocks from PDFs/HTML)
    3. Crawl Planet Smalltalk blog aggregator
    4. Index mailing list archives (extract code snippets)

### Phase 4: Ongoing

    1. Periodic re-crawl of GitHub (daily or weekly for active repos)
    2. Monitor new repos via GitHub event stream or periodic search
    3. Community submissions for uncrawled sources
    4. Manual curation of categories for top packages


## 8. Technical Architecture (Sketch)

    Crawlers          GitHub API crawler, SqueakSource crawler,
                      SmalltalkHub archive crawler, web scraper

    Processing        Dialect detector, auto-categorizer,
                      Monticello .mcz parser, Tonel parser,
                      chunk-format parser, deduplicator

    Storage           Source code store (object storage or filesystem)
                      Metadata database (PostgreSQL)
                      Search index (Elasticsearch, Typesense, or Meilisearch)

    Frontend          Web UI with search bar, faceted filters,
                      code viewer with syntax highlighting,
                      package detail pages with metadata

    API               REST API for programmatic search
                      (could also be consumed by Smalltalk IDEs)


## 9. Feasibility Assessment

    Aspect                  Assessment      Notes
    Data availability       FEASIBLE        ~15,000+ unique packages indexable
    GitHub API access       FEASIBLE        Well-documented, rate limits manageable
    Legacy repo scraping    FEASIBLE        Static archives still accessible
    Dialect detection       FEASIBLE        Strong signals in file formats/metadata
    Auto-categorization     MODERATE        Heuristics + topic tags; manual curation helps
    Web-wide code scraping  MODERATE        Blogs/books extractable; mailing lists noisy
    Unique value prop       STRONG          No existing Smalltalk-specific code search
    Ecosystem size          SMALL BUT OK    Small enough to index comprehensively
    Ongoing maintenance     LOW-MODERATE    Small ecosystem means manageable crawl volume

### Key Advantages

    - The Smalltalk ecosystem is small enough to index comprehensively
    - No competitor exists -- this would be the first dedicated Smalltalk code search
    - GitHub API provides rich metadata for the majority of packages
    - Modern Smalltalk code (Pharo/Cuis) is in git-friendly text formats
    - Community is tight-knit and likely to contribute/promote such a tool

### Key Risks

    - SmalltalkHub archive may go offline (5-year retention from 2020)
    - Some legacy code is in binary formats (parcels, ENVY) and cannot be indexed
    - GitHub Linguist sometimes misclassifies .st files (StringTemplate, HTML)
    - Auto-categorization quality depends on repo descriptions (often sparse)
    - Small user base may limit adoption/contribution


## 10. Conclusion

Building a comprehensive Smalltalk code search engine is feasible. The
ecosystem is small enough (~15,000-18,000 unique packages) to index in its
entirety, GitHub's API provides the bulk of the data with rich metadata,
and no such search engine exists today. The main technical challenges are
dialect detection heuristics, auto-categorization quality, and preserving
access to legacy platform archives before they go offline.

The project would fill a genuine gap in the Smalltalk community's tooling
and could serve as both a discovery tool for developers and a preservation
effort for the language's code heritage.
