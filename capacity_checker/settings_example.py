"""
Settings file example with sensitive information redacted.
Copy this file to settings.py and add your own keys.
"""

import os
from pathlib import Path
import dj_database_url

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'your-secret-key-here'

# Google Maps API Key
GOOGLE_MAPS_API_KEY = 'your-google-maps-api-key'

# Stripe Configuration
STRIPE_PUBLIC_KEY = os.environ.get('STRIPE_PUBLIC_KEY', 'your-stripe-public-key')
STRIPE_SECRET_KEY = os.environ.get('STRIPE_SECRET_KEY', 'your-stripe-secret-key')
# --- Add Webhook Secret --- #
STRIPE_WEBHOOK_SECRET = os.environ.get('STRIPE_WEBHOOK_SECRET') # Get from environment

# Add this setting to your settings.py file
CSRF_TRUSTED_ORIGINS = [
    'https://capacitymarket.co.uk',
    'https://www.capacitymarket.co.uk',
    # Add any other domains you might use
]

# SECURITY WARNING: don't run with debug turned on in production!
# Read DEBUG setting from environment variable (defaults to '0' -> False)
DEBUG = os.environ.get('DEBUG', '0') == '1'  # Uncomment original line
# DEBUG = True  # Comment out force debug True

ALLOWED_HOSTS = ['neso-cmr-search-da0169863eae.herokuapp.com', 'localhost', '127.0.0.1', '.capacitymarket.co.uk', 'www.capacitymarket.co.uk', 'capacitychecker.co.uk', 'www.capacitychecker.co.uk']


# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "checker",
    'django.contrib.humanize',
    'accounts',
    'widget_tweaks',
]

# Conditionally add debug_toolbar if DEBUG is True
if DEBUG:
    INSTALLED_APPS.append('debug_toolbar')

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.gzip.GZipMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",  # Add WhiteNoise for static files in production
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

# Conditionally add Debug Toolbar middleware if DEBUG is True
if DEBUG:
    MIDDLEWARE.insert(MIDDLEWARE.index("django.middleware.clickjacking.XFrameOptionsMiddleware") + 1, "debug_toolbar.middleware.DebugToolbarMiddleware")

ROOT_URLCONF = "capacity_checker.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR.parent / "templates"],  # Add this path to look for templates in the project root
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "accounts.context_processors.account_status_processor",
            ],
        },
    },
]

WSGI_APPLICATION = "capacity_checker.wsgi.application"


# Database
# https://docs.djangoproject.com/en/5.1/ref/settings/#databases

# Default SQLite for development
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / "db.sqlite3", # Use pathlib syntax
    }
}

# Override with DATABASE_URL environment variable if available
if 'DATABASE_URL' in os.environ:
    DATABASES['default'] = dj_database_url.config(
        conn_max_age=600,
        ssl_require=True
    )


# Password validation
# https://docs.djangoproject.com/en/5.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.1/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.1/howto/static-files/

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"  # For Heroku collectstatic
STATICFILES_DIRS = [
    # Point to the project root's static directory
    BASE_DIR.parent / "static",
]

# WhiteNoise configuration for serving static files in production
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

# Default primary key field type
# https://docs.djangoproject.com/en/5.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Update the API_URL with the correct National Grid ESO domain
API_URL = 'https://data.nationalgrideso.com/api/3/action/datastore_search'

# Update your CACHES setting to use the LocMem cache
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
        'TIMEOUT': 3600,  # 1 hour to match map_data_api cache timeout
        'OPTIONS': {
            'MAX_ENTRIES': 1000
        }
    }
}

# --- Logging Configuration ---
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',  # Capture DEBUG level
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO', # Keep Django's own logs at INFO
            'propagate': True,
        },
        'checker': {  # Logger for our app
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
} 