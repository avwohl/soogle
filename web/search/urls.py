from django.urls import path
from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("search/", views.search, name="search"),
    path("package/<int:pk>/", views.package_detail, name="package_detail"),
    path("sources/", views.sources, name="sources"),
    path("videos/", views.videos, name="videos"),
    path("submit/", views.submit_site, name="submit_site"),
]
