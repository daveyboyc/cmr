from django.db import models

# Create your models here.

class Component(models.Model):
    """
    Model for storing components data
    """
    component_id = models.CharField(max_length=100, unique=True, null=True, blank=True, db_index=True)
    cmu_id = models.CharField(max_length=50, db_index=True)  # Already indexed
    location = models.CharField(max_length=255, db_index=True, null=True, blank=True)  # Already indexed
    description = models.TextField(null=True, blank=True)
    technology = models.CharField(max_length=100, db_index=True, null=True, blank=True)  # Already indexed
    company_name = models.CharField(max_length=255, db_index=True, null=True, blank=True)  # Already indexed
    auction_name = models.CharField(max_length=100, null=True, blank=True)
    delivery_year = models.CharField(max_length=50, db_index=True, null=True, blank=True)  # Already indexed
    status = models.CharField(max_length=50, null=True, blank=True)
    type = models.CharField(max_length=50, null=True, blank=True)
    additional_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.cmu_id} - {self.location}"
