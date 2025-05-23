from django.db import models
from django.core.serializers.json import DjangoJSONEncoder

# Create your models here.

class Component(models.Model):
    """
    Model for storing components data
    """
    component_id = models.CharField(max_length=100, unique=True, null=True, blank=True, db_index=True)
    cmu_id = models.CharField(max_length=50, db_index=True)  # Already indexed
    location = models.CharField(max_length=255, db_index=True, null=True, blank=True)  # Already indexed
    description = models.TextField(null=True, blank=True, db_index=True)  # Added index for description searches
    technology = models.CharField(max_length=100, db_index=True, null=True, blank=True)  # Already indexed
    company_name = models.CharField(max_length=255, db_index=True, null=True, blank=True)  # Already indexed
    auction_name = models.CharField(max_length=100, null=True, blank=True, db_index=True)  # Added index
    delivery_year = models.CharField(max_length=50, db_index=True, null=True, blank=True)  # Already indexed
    status = models.CharField(max_length=50, null=True, blank=True)
    type = models.CharField(max_length=50, null=True, blank=True)
    additional_data = models.JSONField(default=dict, encoder=DjangoJSONEncoder)
    # New field for numeric de-rated capacity
    derated_capacity_mw = models.FloatField(null=True, blank=True, db_index=True) 
    
    # Add these new fields for maps
    latitude = models.FloatField(null=True, blank=True, db_index=True)
    longitude = models.FloatField(null=True, blank=True, db_index=True)
    geocoded = models.BooleanField(default=False)  # Track which records have been processed
    
    # Fields for location searching
    county = models.CharField(max_length=100, null=True, blank=True, db_index=True)
    outward_code = models.CharField(max_length=5, null=True, blank=True, db_index=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        # Add compound indexes for common search patterns
        indexes = [
            # Compound index for company search with delivery year (common pattern)
            models.Index(fields=['company_name', 'delivery_year'], name='comp_delivery_idx'),
            
            # Compound index for location-based searches
            models.Index(fields=['location', 'company_name'], name='loc_comp_idx'),
            
            # Compound index for auction searches
            models.Index(fields=['auction_name', 'delivery_year'], name='auction_year_idx'),
            
            # Special index for 'vital' search (company_name LIKE 'VITAL ENERGI%' AND location NOT LIKE '%Leeds%')
            # This partial index would be ideal but requires PostgreSQL, 
            # so we'll create a regular compound index instead
            models.Index(fields=['company_name', 'location'], name='vital_search_idx'),
            
            # Add spatial index for map viewport queries
            models.Index(fields=['latitude', 'longitude'], name='spatial_idx'),
            
            # New composite index for technology and company_name
            models.Index(fields=['technology', 'company_name'], name='tech_comp_idx'),

            # New composite index for delivery_year and location
            models.Index(fields=['delivery_year', 'location'], name='year_loc_idx'),
        ]
        
        # Add database optimizations
        ordering = ['-delivery_year']  # Default ordering

    def __str__(self):
        return f"{self.cmu_id} - {self.component_id} ({self.location[:30]})"

    # Optional - add a method to get map info
    def map_info(self):
        """Return a dict with info needed for map markers"""
        return {
            'id': self.id,
            'title': self.location or 'Unknown Location',
            'description': self.description or '',
            'technology': self.technology or 'Unknown',
            'company': self.company_name or 'Unknown',
            'cmu_id': self.cmu_id,
            'lat': self.latitude,
            'lng': self.longitude,
            'delivery_year': self.delivery_year or '',
            'url': f'/component/{self.id}/'  # Assuming you have a detail view URL named 'component_detail'
        }


class CMURegistry(models.Model):
    cmu_id = models.CharField(max_length=100, primary_key=True, unique=True)
    raw_data = models.JSONField(default=dict, encoder=DjangoJSONEncoder)
    last_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        applicant = self.raw_data.get('Name of Applicant', 'Unknown')
        return f"{self.cmu_id} ({applicant})"
