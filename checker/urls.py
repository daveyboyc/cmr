from django.urls import path
from . import views
from .services.component_detail import get_component_details
from .debug_views import debug_component_duplicates

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

    # API endpoint for getting auction components
    path("api/auction-components/<str:company_id>/<str:year>/<str:auction_name>/", views.auction_components, name="auction_components_api"),
    
    # Debug endpoint for troubleshooting component issues
    path("debug/auction-components/<str:company_id>/<str:year>/<str:auction_name>/", views.debug_auction_components, name="debug_auction_components"),

    # Debug endpoints
    path("debug/cache/<str:cmu_id>/",
         views.debug_cache, name="debug_cache"),
    path("debug/mapping/",
         views.debug_mapping, name="debug_mapping"),
    path("debug/mapping-cache/",
         views.debug_mapping_cache, name="debug_mapping_cache"),
         
    # New debug endpoint for component investigation
    path("debug/component-retrieval/<str:cmu_id>/",
         views.debug_component_retrieval, name="debug_component_retrieval"),
         
    # Debug endpoint for company components
    path("debug/company-components/",
         views.debug_company_components, name="debug_company_components"),

    # Debug URLs
    path('debug/duplicates/<str:cmu_id>/', debug_component_duplicates, name='debug_duplicates'),
    path('statistics/', views.statistics_view, name='statistics'),
]