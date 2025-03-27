from django.db import models

# Create your models here.

class Component(models.Model):
    component_id = models.CharField(max_length=100, unique=True, null=True, blank=True)
    cmu_id = models.CharField(max_length=50, db_index=True)
    location = models.CharField(max_length=255, db_index=True, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    technology = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    company_name = models.CharField(max_length=255, db_index=True, null=True, blank=True)
    auction_name = models.CharField(max_length=100, null=True, blank=True)
    delivery_year = models.CharField(max_length=50, db_index=True, null=True, blank=True)
    status = models.CharField(max_length=50, null=True, blank=True)
    type = models.CharField(max_length=50, null=True, blank=True)
    additional_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
