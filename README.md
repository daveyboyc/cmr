## Redis Caching System

### Overview
The application uses Redis for performance optimization in several areas:
- **Location mapping**: ~14.5K locations mapped to postcodes (speeds up location searches)
- **Map cache**: Pre-rendered technology map data for various zoom levels
- **CMU dataframe**: Cached version of the pandas dataframe with 15,338 records
- **Company index**: 1,545 companies with normalized names and pre-rendered HTML
- **Component details**: Cached component details for map markers

### Performance Improvements
- Location search: from 5s to milliseconds
- CMU dataframe loading: saving ~1.4s per search
- Company link building: from ~2.9s to ~0.022s per search
- Map rendering: from 1-2s to ~200ms per load

### Rebuilding Redis Caches
After a fresh checkout or deployment, run:
```
./rebuild_redis_caches.sh
```

This script will rebuild:
1. The full location mapping (~14.5K locations)
2. Map cache for all technologies and zoom levels
3. Verify the cache status when complete

### Cache Status
To check the status of all Redis caches:
```
python manage.py check_cache_status 