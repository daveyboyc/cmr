import os
import dj_database_url  # Make sure you have this import

# Replace your existing DATABASES setting with this more comprehensive one
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
        'CONN_MAX_AGE': 600,  # Keep connections alive for 10 minutes
        'OPTIONS': {
            'connect_timeout': 10,  # SQLite timeout in seconds
            'statement_timeout': 15000,  # 15 seconds query timeout
        }
    }
}

# If using PostgreSQL via DATABASE_URL (Heroku), it will override this base config
if 'DATABASE_URL' in os.environ:
    DATABASES['default'] = dj_database_url.config(
        conn_max_age=600,
        ssl_require=True,
        options={
            'options': '-c statement_timeout=25000',  # 25 seconds query timeout
            'connect_timeout': 10,                     # 10 seconds connection timeout
        }
    )

# Set a modest timeout for cache operations
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
        'TIMEOUT': 3600,  # 1 hour (in seconds)
        'OPTIONS': {
            'MAX_ENTRIES': 10000,
            'CULL_FREQUENCY': 3,  # Purge 1/3 of entries when MAX_ENTRIES is reached
        }
    }
} 