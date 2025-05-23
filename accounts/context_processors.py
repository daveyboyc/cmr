from .models import UserProfile
import logging

logger = logging.getLogger(__name__)

def account_status_processor(request):
    """Adds user profile and payment status to the template context."""
    has_paid_access = False # Default for anonymous users or users without profile
    if request.user.is_authenticated:
        try:
            profile = request.user.profile
            has_paid_access = profile.is_paid_access_active
        except UserProfile.DoesNotExist:
            # This case should be rare due to the signal, but handle it.
            logger.warning(f"UserProfile not found for authenticated user {request.user.username} in context processor.")
            # Optionally create profile here if needed, but signal should handle it.
            # profile = UserProfile.objects.create(user=request.user)
            # has_paid_access = False 
            pass # Keep has_paid_access as False
            
    return {
        'has_paid_access': has_paid_access
    } 