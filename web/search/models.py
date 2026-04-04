"""Read-only models mapping to the existing soogle database."""

from django.db import models


class Site(models.Model):
    name = models.CharField(max_length=100, unique=True)
    display_name = models.CharField(max_length=200)
    base_url = models.CharField(max_length=500)
    site_type = models.CharField(max_length=20)
    is_active = models.BooleanField(default=True)

    class Meta:
        managed = False
        db_table = "sites"

    def __str__(self):
        return self.display_name


class Package(models.Model):
    name = models.CharField(max_length=500)
    qualified_name = models.CharField(max_length=500)
    description = models.TextField(blank=True)
    dialect = models.CharField(max_length=30)
    dialect_confidence = models.SmallIntegerField(default=0)
    file_format = models.CharField(max_length=30)
    site = models.ForeignKey(Site, on_delete=models.DO_NOTHING)
    external_id = models.CharField(max_length=500)
    url = models.CharField(max_length=1000, blank=True)
    clone_url = models.CharField(max_length=1000, blank=True)
    stars = models.IntegerField(default=0)
    forks = models.IntegerField(default=0)
    size_kb = models.IntegerField(default=0)
    license = models.CharField(max_length=200, blank=True)
    is_fork = models.BooleanField(default=False)
    is_archived = models.BooleanField(default=False)
    default_branch = models.CharField(max_length=200, blank=True)
    topics = models.JSONField(default=list, blank=True)
    source_created_at = models.DateTimeField(null=True)
    source_updated_at = models.DateTimeField(null=True)
    source_pushed_at = models.DateTimeField(null=True)
    is_active = models.BooleanField(default=True)
    readme_excerpt = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = "packages"

    def __str__(self):
        return self.name


class Category(models.Model):
    name = models.CharField(max_length=100, unique=True)
    display_name = models.CharField(max_length=200)
    description = models.TextField(blank=True)
    sort_order = models.IntegerField(default=0)

    class Meta:
        managed = False
        db_table = "categories"
        ordering = ["sort_order"]

    def __str__(self):
        return self.display_name


class PackageCategory(models.Model):
    package = models.ForeignKey(Package, on_delete=models.DO_NOTHING)
    category = models.ForeignKey(Category, on_delete=models.DO_NOTHING)
    confidence = models.SmallIntegerField(default=0)

    class Meta:
        managed = False
        db_table = "package_categories"


class PackageClass(models.Model):
    package = models.ForeignKey(Package, on_delete=models.DO_NOTHING, related_name="classes")
    class_name = models.CharField(max_length=500)
    superclass_name = models.CharField(max_length=500, blank=True)
    category = models.CharField(max_length=500, blank=True)
    is_trait = models.BooleanField(default=False)

    class Meta:
        managed = False
        db_table = "package_classes"

    def __str__(self):
        return self.class_name


class PackageMethod(models.Model):
    package = models.ForeignKey(Package, on_delete=models.DO_NOTHING, related_name="methods")
    class_field = models.ForeignKey(PackageClass, on_delete=models.DO_NOTHING, db_column="class_id")
    selector = models.CharField(max_length=500)
    protocol = models.CharField(max_length=500, blank=True)
    is_class_side = models.BooleanField(default=False)
    source_code = models.TextField(blank=True)

    class Meta:
        managed = False
        db_table = "package_methods"

    def __str__(self):
        return self.selector


class Video(models.Model):
    title = models.CharField(max_length=500)
    description = models.TextField(blank=True)
    url = models.CharField(max_length=1000)
    video_id = models.CharField(max_length=100, unique=True)
    channel_name = models.CharField(max_length=500, blank=True)
    channel_url = models.CharField(max_length=1000, blank=True)
    thumbnail_url = models.CharField(max_length=1000, blank=True)
    duration_seconds = models.IntegerField(null=True)
    published_at = models.DateTimeField(null=True)
    view_count = models.IntegerField(default=0)
    dialect = models.CharField(max_length=30, default="unknown")
    source = models.CharField(max_length=100, default="youtube")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = "videos"

    def __str__(self):
        return self.title


class SiteSubmission(models.Model):
    url = models.CharField(max_length=2000)
    comment = models.TextField(blank=True, default="")
    ip_address = models.CharField(max_length=45, blank=True, default="")
    status = models.CharField(max_length=10, default="pending")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = "site_submissions"

    def __str__(self):
        return self.url
