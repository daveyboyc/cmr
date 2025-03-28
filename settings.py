import os
import dj_database_url  # Make sure you have this import

# Replace your existing DATABASES setting with this more comprehensive one
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
        'CONN_MAX_AGE': 600,  # Keep connections alive for 10 minutes
        'OPTIONS': {
            'timeout': 20,  # SQLite timeout in seconds
        }
    }
}

# If using PostgreSQL via DATABASE_URL (Heroku), it will override this base config
if 'DATABASE_URL' in os.environ:
    DATABASES['default'] = dj_database_url.config(
        conn_max_age=600,
        ssl_require=True,
        options='-c statement_timeout=15000'  # 15 second query timeout for PostgreSQL
    )

# Set a shorter timeout for the cache
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
        'TIMEOUT': 300,  # 5 minutes
        'OPTIONS': {
            'MAX_ENTRIES': 1000
        }
    }
} 