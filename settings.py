DATABASES = {
    'default': {
        # ... your existing database settings
        'CONN_MAX_AGE': 600,  # Keep connections alive for 10 minutes
        'OPTIONS': {
            'connect_timeout': 5,
        }
    }
} 