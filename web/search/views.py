from django.shortcuts import render, get_object_or_404, redirect
from django.core.paginator import Paginator
from django.db.models import Q, Count
from .models import Package, Category, Site, PackageCategory, PackageClass, PackageMethod, SiteSubmission, Video


def index(request):
    """Home page with search box and stats."""
    stats = {
        "packages": Package.objects.count(),
        "sites": Site.objects.filter(is_active=True).count(),
        "videos": Video.objects.count(),
    }
    # Dialect breakdown (skip 'unknown')
    from django.db import connection
    with connection.cursor() as cur:
        cur.execute(
            "SELECT dialect, COUNT(*) AS n FROM packages "
            "WHERE dialect != 'unknown' GROUP BY dialect ORDER BY n DESC"
        )
        stats["dialects"] = cur.fetchall()

    # Recent packages
    recent = Package.objects.order_by("-created_at")[:10]

    return render(request, "search/index.html", {"stats": stats, "recent": recent})


def search(request):
    """Full-text search across packages."""
    q = request.GET.get("q", "").strip()
    dialect = request.GET.get("dialect", "")
    site = request.GET.get("site", "")
    category = request.GET.get("category", "")
    sort = request.GET.get("sort", "relevance")
    page_num = request.GET.get("page", 1)

    qs = Package.objects.select_related("site")

    if q:
        # Use MySQL MATCH ... AGAINST for fulltext when available
        qs = qs.filter(Q(name__icontains=q) | Q(description__icontains=q))

    if dialect:
        qs = qs.filter(dialect=dialect)

    if site:
        qs = qs.filter(site__name=site)

    if category:
        pkg_ids = PackageCategory.objects.filter(
            category__name=category
        ).values_list("package_id", flat=True)
        qs = qs.filter(id__in=pkg_ids)

    if sort == "stars":
        qs = qs.order_by("-stars", "-source_pushed_at")
    elif sort == "updated":
        qs = qs.order_by("-source_pushed_at")
    elif sort == "name":
        qs = qs.order_by("name")
    else:
        # Default: name match quality isn't easily sortable with icontains,
        # so fall back to stars then name
        if q:
            qs = qs.order_by("-stars", "name")
        else:
            qs = qs.order_by("name")

    paginator = Paginator(qs, 25)
    page = paginator.get_page(page_num)

    # Filter options
    dialects = (
        Package.objects.values_list("dialect", flat=True)
        .distinct()
        .order_by("dialect")
    )
    sites = Site.objects.filter(is_active=True).order_by("display_name")
    categories = Category.objects.order_by("sort_order")

    return render(request, "search/results.html", {
        "q": q,
        "dialect": dialect,
        "site": site,
        "category": category,
        "sort": sort,
        "page": page,
        "paginator": paginator,
        "dialects": dialects,
        "sites": sites,
        "categories": categories,
    })


def package_detail(request, pk):
    """Detail view for a single package."""
    pkg = get_object_or_404(Package.objects.select_related("site"), pk=pk)

    # Categories
    cats = Category.objects.filter(
        id__in=PackageCategory.objects.filter(package=pkg).values_list("category_id", flat=True)
    )

    # Classes and methods
    classes = PackageClass.objects.filter(package=pkg).order_by("class_name")
    methods = PackageMethod.objects.filter(package=pkg).select_related("class_field").order_by(
        "class_field__class_name", "selector"
    )

    return render(request, "search/detail.html", {
        "pkg": pkg,
        "categories": cats,
        "classes": classes,
        "methods": methods,
    })


def sources(request):
    """List all active source sites."""
    sites = (
        Site.objects.filter(is_active=True)
        .annotate(package_count=Count("package"))
        .order_by("-package_count")
    )
    return render(request, "search/sources.html", {"sites": sites})


def videos(request):
    """List Smalltalk videos."""
    q = request.GET.get("q", "").strip()
    dialect = request.GET.get("dialect", "")
    sort = request.GET.get("sort", "views")
    page_num = request.GET.get("page", 1)

    qs = Video.objects.all()

    if q:
        qs = qs.filter(Q(title__icontains=q) | Q(description__icontains=q))

    if dialect:
        qs = qs.filter(dialect=dialect)

    if sort == "newest":
        qs = qs.order_by("-published_at")
    elif sort == "title":
        qs = qs.order_by("title")
    else:
        qs = qs.order_by("-view_count")

    paginator = Paginator(qs, 24)
    page = paginator.get_page(page_num)

    dialects = (
        Video.objects.exclude(dialect="unknown")
        .values_list("dialect", flat=True)
        .distinct()
        .order_by("dialect")
    )

    return render(request, "search/videos.html", {
        "q": q,
        "dialect": dialect,
        "sort": sort,
        "page": page,
        "paginator": paginator,
        "dialects": dialects,
    })


def submit_site(request):
    """Form to submit a URL the user thinks we should index."""
    if request.method == "POST":
        url = request.POST.get("url", "").strip()
        comment = request.POST.get("comment", "").strip()
        if url:
            ip = request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")[0].strip()
            if not ip:
                ip = request.META.get("REMOTE_ADDR", "")
            SiteSubmission.objects.create(url=url[:2000], comment=comment[:5000], ip_address=ip)
            return render(request, "search/submit_thanks.html", {"url": url})

    return render(request, "search/submit.html")
