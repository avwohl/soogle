import logging

import requests
from django.conf import settings
from django.core.mail import send_mail
from django.http import HttpResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.core.paginator import Paginator
from django.db.models import Q, Count
from django.utils.html import escape as html_escape
from .models import Package, Category, Site, PackageCategory, PackageClass, PackageMethod, SiteSubmission, Video

log = logging.getLogger(__name__)

SITEMAP_CHUNK = 10000


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
        # Default: stars then name — surfaces quality content first
        qs = qs.order_by("-stars", "name")

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


def _verify_hcaptcha(token, remote_ip):
    """Return True if hCaptcha accepts the token (or if hCaptcha isn't configured)."""
    secret = settings.HCAPTCHA_SECRET
    if not secret:
        return True  # not configured — skip verification
    if not token:
        return False
    try:
        resp = requests.post(
            "https://hcaptcha.com/siteverify",
            data={"secret": secret, "response": token, "remoteip": remote_ip},
            timeout=10,
        )
        resp.raise_for_status()
        return bool(resp.json().get("success"))
    except Exception as e:
        log.warning("hCaptcha verification error: %s", e)
        return False


def _notify_submission(submission):
    """Email the admin when a new URL is submitted.  Failure is non-fatal."""
    to_addr = getattr(settings, "SUBMISSION_EMAIL_TO", "")
    if not to_addr:
        return
    body = (
        f"A new URL was submitted to soogle.org/submit/.\n\n"
        f"URL:     {submission.url}\n"
        f"IP:      {submission.ip_address or 'unknown'}\n"
        f"When:    {submission.created_at}\n\n"
        f"Comment:\n{submission.comment or '(none)'}\n"
    )
    try:
        send_mail(
            subject=f"[soogle] New URL submission: {submission.url[:80]}",
            message=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[to_addr],
            fail_silently=False,
        )
    except Exception as e:
        log.warning("Failed to send submission notification email: %s", e)


def submit_site(request):
    """Form to submit a URL the user thinks we should index."""
    ctx = {"hcaptcha_sitekey": settings.HCAPTCHA_SITEKEY}

    if request.method == "POST":
        url = request.POST.get("url", "").strip()
        comment = request.POST.get("comment", "").strip()
        honeypot = request.POST.get("website", "").strip()
        token = request.POST.get("h-captcha-response", "")

        ip = request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")[0].strip()
        if not ip:
            ip = request.META.get("REMOTE_ADDR", "")

        if honeypot:
            # Bot filled the hidden field — pretend success without saving.
            return render(request, "search/submit_thanks.html", {"url": url})

        if not url:
            ctx.update({"error": "Please enter a URL.", "url": url, "comment": comment})
            return render(request, "search/submit.html", ctx)

        if not _verify_hcaptcha(token, ip):
            ctx.update({"error": "Captcha verification failed. Please try again.",
                        "url": url, "comment": comment})
            return render(request, "search/submit.html", ctx)

        submission = SiteSubmission.objects.create(
            url=url[:2000], comment=comment[:5000], ip_address=ip
        )
        _notify_submission(submission)
        return render(request, "search/submit_thanks.html", {"url": url})

    return render(request, "search/submit.html", ctx)


def robots_txt(request):
    base = f"{request.scheme}://{request.get_host()}"
    content = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /submit/\n"
        "\n"
        f"Sitemap: {base}/sitemap.xml\n"
    )
    return HttpResponse(content, content_type="text/plain")


def sitemap_xml(request):
    """Sitemap index pointing to section sitemaps."""
    base = f"{request.scheme}://{request.get_host()}"
    pkg_count = Package.objects.count()
    chunks = (pkg_count + SITEMAP_CHUNK - 1) // SITEMAP_CHUNK

    lines = ['<?xml version="1.0" encoding="UTF-8"?>']
    lines.append('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    lines.append(f"  <sitemap><loc>{base}/sitemap-pages.xml</loc></sitemap>")
    for i in range(1, chunks + 1):
        lines.append(f"  <sitemap><loc>{base}/sitemap-packages-{i}.xml</loc></sitemap>")
    lines.append("</sitemapindex>")
    return HttpResponse("\n".join(lines), content_type="application/xml")


def sitemap_pages(request):
    """Static pages sitemap."""
    base = f"{request.scheme}://{request.get_host()}"
    pages = [
        ("/", "daily", "1.0"),
        ("/search/", "daily", "0.9"),
        ("/videos/", "daily", "0.8"),
        ("/sources/", "weekly", "0.7"),
    ]
    lines = ['<?xml version="1.0" encoding="UTF-8"?>']
    lines.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    for path, freq, priority in pages:
        lines.append(
            f"  <url><loc>{base}{path}</loc>"
            f"<changefreq>{freq}</changefreq>"
            f"<priority>{priority}</priority></url>"
        )
    lines.append("</urlset>")
    return HttpResponse("\n".join(lines), content_type="application/xml")


def sitemap_packages(request, page):
    """Package detail pages sitemap, chunked by SITEMAP_CHUNK."""
    base = f"{request.scheme}://{request.get_host()}"
    offset = (page - 1) * SITEMAP_CHUNK
    pkgs = (
        Package.objects.order_by("id")
        .values_list("id", "updated_at")[offset : offset + SITEMAP_CHUNK]
    )
    lines = ['<?xml version="1.0" encoding="UTF-8"?>']
    lines.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    for pk, updated_at in pkgs:
        loc = f"{base}/package/{pk}/"
        lastmod = ""
        if updated_at:
            lastmod = f"<lastmod>{updated_at.strftime('%Y-%m-%d')}</lastmod>"
        lines.append(
            f"  <url><loc>{loc}</loc>{lastmod}"
            f"<changefreq>weekly</changefreq><priority>0.6</priority></url>"
        )
    lines.append("</urlset>")
    return HttpResponse("\n".join(lines), content_type="application/xml")
