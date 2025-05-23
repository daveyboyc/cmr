from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.models import User
from .models import UserProfile
import logging

logger = logging.getLogger(__name__)

@receiver(post_save, sender=User)
def create_or_update_user_profile(sender, instance, created, **kwargs):
    """
    Create or update the user profile when a User object is saved.
    Creates a profile for new users.
    Ensures existing users have a profile (in case it was missed).
    """
    if created:
        # Check if profile already exists (shouldn't for created=True, but safety check)
        if not hasattr(instance, 'profile'):
            UserProfile.objects.create(user=instance)
            logger.info(f"Created UserProfile for newly created user: {instance.username}")
        else:
             logger.warning(f"UserProfile already existed for newly created user: {instance.username}")
    else:
        # If user is updated, ensure profile exists (e.g., for users created before profile model)
        try:
            # Attempt to access the profile. If it exists, do nothing.
            _ = instance.profile 
        except UserProfile.DoesNotExist:
            # If profile does not exist for an existing user, create it.
            UserProfile.objects.create(user=instance)
            logger.warning(f"Created missing UserProfile for existing user: {instance.username}") 