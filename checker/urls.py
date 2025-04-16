from django.urls import path
from . import views
from .services.component_detail import get_component_details
from .debug_views import debug_component_duplicates
from .services.company_search import search_companies_service

urlpatterns = [
    path("", search_companies_service, name="search_companies"),
    path("components/", views.search_components, name="search_components"),

    # HTMX endpoints
    path("api/company-years/<str:company_id>/<str:year>/",
         views.htmx_company_years, name="htmx_company_years"),
    path("api/company-years/<str:company_id>/<str:year>/<str:auction_name>/",
         views.htmx_company_years, name="htmx_company_years_with_auction"),
    path("api/auction-components/<str:company_id>/<str:year>/<str:auction_name>/",
         views.htmx_auction_components, name="htmx_auction_components"),
    path("api/cmu-details/<str:cmu_id>/",
         views.get_cmu_details, name="htmx_cmu_details"),

    # Component detail page - use integer primary key
    path("component/<int:pk>/",
         views.component_detail, name="component_detail"),
    path("component/by-id/<str:component_id>/",
         views.component_detail_by_id, name="component_detail_by_id"),

    # Company detail page
    path("company/<str:company_id>/",
         views.company_detail, name="company_detail"),

    # Map view and API
    path('map/', views.map_view, name='map_view'),
    path('api/map-data/', views.map_data_api, name='map_data_api'),

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
#    path("debug/mapping/",
#         views.debug_mapping, name="debug_mapping"),
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
    
    # Index information endpoint
    path('debug/indexes/', views.index_info, name='index_info'),
    
    # New URL for technology-specific search results - Reverted back to PATH
    path('technology/<path:technology_name_encoded>/', views.technology_search_results, name='technology_search'),

    # New URL for full de-rated capacity list (components)
    path('components/by-derated-capacity/', views.derated_capacity_list, name='derated_capacity_list'),

    # New URL for full company list by total capacity
    path('companies/by-total-capacity/', views.company_capacity_list, name='company_capacity_list'),

    # Placeholder URL for full technology list
    path('technologies/', views.technology_list_view, name='technology_list'), # NOW points to the real view
]