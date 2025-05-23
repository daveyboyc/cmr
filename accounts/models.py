from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

# Create your models here.


class UserProfile(models.Model):
    """Extends the default User model to include application-specific fields."""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')
    has_paid_access = models.BooleanField(default=False, help_text="Indicates if the user has active paid access.")
    paid_access_expiry_date = models.DateTimeField(null=True, blank=True, help_text="The date and time when paid access expires.")

    def __str__(self):
        return f"{self.user.username}'s Profile"

    @property
    def is_paid_access_active(self):
        """Checks if the user currently has active paid access."""
        if not self.has_paid_access:
            return False
        if self.paid_access_expiry_date is None:
            # If expiry is null but has_paid_access is True, assume perpetual access (or handle as needed)
            return True
        return timezone.now() < self.paid_access_expiry_date


class RegistrationEmailRecord(models.Model):
    """Keeps a record of all registration emails sent, even if user accounts weren't created."""
    email = models.EmailField()
    timestamp = models.DateTimeField(auto_now_add=True)
    activation_link = models.TextField(blank=True, null=True)
    user_created = models.BooleanField(default=False)
    user_activated = models.BooleanField(default=False)
    error_message = models.TextField(blank=True, null=True)
    
    class Meta:
        ordering = ['-timestamp']
        verbose_name = "Registration Email Record"
        verbose_name_plural = "Registration Email Records"
    
    def __str__(self):
        status = "activated" if self.user_activated else ("created" if self.user_created else "pending")
        return f"{self.email} ({status}) - {self.timestamp.strftime('%Y-%m-%d %H:%M')}"
