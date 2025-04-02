"""
URL configuration for capacity_checker project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
import sys
from capacity_checker.checker import views

print("DEBUG: Loading main urls.py", file=sys.stderr)

urlpatterns = [
    path('admin/', admin.site.urls),
    path("", include("checker.urls")),
   # path('debug-mapping/', views.debug_mapping_cache, name='debug-mapping'),
   # path('test-cache/', views.test_cache, name='test_cache'),
]
