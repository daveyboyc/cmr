"""
WSGI config for capacity_checker project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/howto/deployment/wsgi/
"""

import os
import sys
from pathlib import Path

# Add the parent directory (containing the 'checker' app) to sys.path
BASE_DIR_PARENT = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR_PARENT))

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")

application = get_wsgi_application()
