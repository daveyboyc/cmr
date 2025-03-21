from django.urls import path
from . import views

urlpatterns = [
    path("", views.search_companies, name="search_companies"),
    path("components/", views.search_components, name="search_components"),

    # HTMX endpoints
    path("api/company-years/<str:company_id>/<str:year>/",
         views.htmx_company_years, name="htmx_company_years"),
    path("api/company-years/<str:company_id>/<str:year>/<str:auction_name>/",
         views.htmx_company_years, name="htmx_company_years_with_auction"),
    path("api/auction-components/<str:company_id>/<str:year>/<str:auction_name>/",
         views.htmx_auction_components, name="htmx_auction_components"),
    path("api/cmu-details/<str:cmu_id>/",
         views.htmx_cmu_details, name="htmx_cmu_details"),

    # Component detail page
    path("component/<str:component_id>/",
         views.component_detail, name="component_detail"),

    # Company detail page
    path("company/<str:company_id>/",
         views.company_detail, name="company_detail"),

    # Debug/admin endpoints
    path("debug/mapping-cache/",
         views.debug_mapping_cache, name="debug_mapping_cache"),
]