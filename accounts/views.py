from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.core.mail import send_mail
from django.template.loader import render_to_string
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode
from django.utils.encoding import force_bytes, force_str
from django.contrib.auth.tokens import default_token_generator
from django.contrib.auth.models import User
from django.conf import settings
from .forms import UserRegistrationForm
from django.contrib.auth import login # Need login here now
from django.urls import reverse # Add this import for reverse URL lookups
import logging # Add logging import
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from .models import UserProfile, RegistrationEmailRecord
import stripe
from django.http import HttpResponse, HttpResponseRedirect
from django.views.decorators.http import require_POST
from django.contrib.auth.views import PasswordChangeView
from django.urls import reverse_lazy

# Create logger
logger = logging.getLogger(__name__)

# Create your views here.

@csrf_exempt  # This exempts this view from CSRF verification (temporary fix)
def register(request):
    if request.method == 'POST':
        form = UserRegistrationForm(request.POST)
        if form.is_valid():
            email = form.cleaned_data['email']
            logger.info(f"Registration attempt with email: {email}")
            
            # Create an email record entry regardless of what happens next
            email_record = RegistrationEmailRecord(email=email)
            
            # Initialize existing_user to None
            existing_user = None
            
            # Check if a user with this email already exists but is inactive
            try:
                existing_user = User.objects.get(username=email)
                if not existing_user.is_active:
                    # User exists but is inactive, we can reuse this account
                    # Update the password to the new one
                    existing_user.set_password(form.cleaned_data['password1'])
                    existing_user.save()
                    user = existing_user
                    logger.info(f"Reusing inactive user: {user.username}")
                    email_record.user_created = True
                else:
                    # User exists and is active
                    logger.warning(f"Attempted registration with active email: {email}")
                    messages.error(request, f'The email address {email} is already registered and active. Please use the login page instead or try a different email address.')
                    
                    # Update email record with error
                    email_record.error_message = "Attempted registration with already active email"
                    email_record.save()
                    
                    return render(request, 'accounts/register.html', {'form': form})
            except User.DoesNotExist:
                # Create a new user
                user = form.save(commit=False) # Don't save to DB yet
                user.is_active = False # Deactivate account until email confirmation
                user.save() # Save user with is_active=False
                logger.info(f"Created inactive user: {user.username} with email: {user.email}")
                email_record.user_created = True

                # Send admin notification about new registration
                admin_subject = f'New User Registration: {email}'
                admin_message = f'A new user has registered with email: {email}. The account is pending email verification.'
                try:
                    send_mail(
                        admin_subject,
                        admin_message,
                        settings.DEFAULT_FROM_EMAIL,
                        [settings.DEFAULT_FROM_EMAIL],  # Send to admin email
                        fail_silently=True
                    )
                    logger.info(f"Admin notification sent for new registration: {email}")
                except Exception as e:
                    logger.error(f"Failed to send admin notification for new registration: {str(e)}")

            # Prepare email
            subject = 'Activate Your Account'
            uid = urlsafe_base64_encode(force_bytes(user.pk))
            token = default_token_generator.make_token(user)
            activation_url = reverse('accounts:activate', kwargs={'uidb64': uid, 'token': token})
            activation_link = f"{settings.SITE_SCHEME}://{settings.SITE_DOMAIN}{activation_url}"
            
            # Save activation link to email record
            email_record.activation_link = activation_link
            
            logger.info(f"Generated activation link: {activation_link}")

            message = render_to_string('accounts/activation_email.html', {
                'user': user,
                'activation_link': activation_link,
            })

            try:
                # Log email settings being used
                logger.info(f"Email settings: HOST={settings.EMAIL_HOST}, PORT={settings.EMAIL_PORT}, "
                           f"USER={settings.EMAIL_HOST_USER}, FROM={settings.DEFAULT_FROM_EMAIL}")
                
                # Attempt to send email
                send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [user.email])
                logger.info(f"Successfully sent activation email to {user.email}")
                
                # Update and save email record
                email_record.save()
                
                messages.success(request, 'Registration successful! Please check your email to activate your account.')
                # Redirect to a confirmation pending page or home
                return redirect('accounts:registration_pending')
            except Exception as e:
                logger.error(f"Error sending activation email: {str(e)}", exc_info=True)
                messages.error(request, f'Error sending activation email: {e}')
                
                # Update email record with error
                email_record.error_message = f"Error sending activation email: {str(e)}"
                email_record.save()
                
                # Optionally delete the user or handle the error differently
                if not existing_user:  # Only delete if this was a new user
                    user.delete() # Simple cleanup on email error
                    logger.info(f"Deleted user {user.username} due to email sending failure")
                    email_record.user_created = False
                    email_record.save()
                return render(request, 'accounts/register.html', {'form': form})

        else:
            # Form is invalid, render it again with errors
            logger.info(f"Invalid registration form. Errors: {form.errors}")
            return render(request, 'accounts/register.html', {'form': form})
    else:
        # GET request, show the blank form
        form = UserRegistrationForm()
    return render(request, 'accounts/register.html', {'form': form})

def activate(request, uidb64, token):
    try:
        uid = force_str(urlsafe_base64_decode(uidb64))
        user = User.objects.get(pk=uid)
    except (TypeError, ValueError, OverflowError, User.DoesNotExist):
        user = None

    if user is not None and default_token_generator.check_token(user, token):
        user.is_active = True
        user.save()
        login(request, user) # Log the user in after activation
        messages.success(request, 'Account activated successfully! You are now logged in.')
        
        # Update any email records for this user
        RegistrationEmailRecord.objects.filter(email=user.email).update(user_activated=True)
        
        # Send admin notification about successful activation
        admin_subject = f'User Account Activated: {user.email}'
        admin_message = f'The user with email: {user.email} has successfully activated their account.'
        try:
            send_mail(
                admin_subject,
                admin_message,
                settings.DEFAULT_FROM_EMAIL,
                [settings.DEFAULT_FROM_EMAIL],  # Send to admin email
                fail_silently=True
            )
            logger.info(f"Admin notification sent for account activation: {user.email}")
        except Exception as e:
            logger.error(f"Failed to send admin notification for account activation: {str(e)}")
        
        # Redirect to home or a dashboard page
        return redirect('search_companies') # Using the root URL name instead of 'home'
    else:
        messages.error(request, 'Activation link is invalid or has expired.')
        # Redirect to a page indicating activation failure
        return redirect('accounts:activation_failed')

# Add the missing views
def registration_pending(request):
    return render(request, 'accounts/registration_pending.html')

def activation_failed(request):
    return render(request, 'accounts/activation_failed.html')

def must_register(request):
    """Displays a page informing the user they must register to continue."""
    return render(request, 'accounts/must_register.html')

# --- Account Page --- #
@login_required
def account_view(request):
    """Displays the user's account page."""
    try:
        # Fetch the related UserProfile
        profile = request.user.profile
    except UserProfile.DoesNotExist:
        # This should ideally not happen due to the signal, but handle it defensively
        logger.error(f"UserProfile not found for user {request.user.username}. Creating now.")
        profile = UserProfile.objects.create(user=request.user)
        
    context = {
        'user': request.user,
        'profile': profile,
        # Pass the specific boolean value to the template
        'has_paid_access': profile.is_paid_access_active 
    }
    return render(request, 'accounts/account.html', context)

# --- Payment Required Page --- #
@login_required
def payment_required_view(request):
    """Displays a page informing the user their free access has expired."""
    # You could potentially pass extra context if needed, e.g., product info
    context = {}
    return render(request, 'accounts/payment_required.html', context)

# --- Stripe Payment Initiation --- #
@login_required
def initiate_payment_view(request):
    """Initiates the Stripe Checkout session for one-time payment."""
    stripe.api_key = settings.STRIPE_SECRET_KEY
    # *** Replace with your actual Price ID ***
    # PRICE_ID = 'price_REPLACE_ME' # Placeholder
    PRICE_ID = 'price_1RJyMQIn9WiPSMiwDoh0dLTv' # Update this with your new one-time payment price ID

    if PRICE_ID == 'price_REPLACE_ME':
        # Safety check - don't proceed without a real Price ID
        messages.error(request, "Stripe Price ID is not configured.")
        # Redirect back to account page or payment required page
        return redirect('accounts:payment_required') 

    # Build absolute URLs for success/cancel
    # Use request.build_absolute_uri() to handle http/https correctly
    success_url = request.build_absolute_uri(reverse('accounts:account') + '?payment=success')
    cancel_url = request.build_absolute_uri(reverse('accounts:payment_required'))

    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[
                {
                    'price': PRICE_ID,
                    'quantity': 1,
                },
            ],
            mode='payment',  # Changed from 'subscription' to 'payment' for one-time payment
            success_url=success_url,
            cancel_url=cancel_url,
            # Prefill email and include user ID for webhook
            customer_email=request.user.email, 
            metadata={
                'user_id': request.user.id
            }
        )
        # Redirect to Stripe Checkout
        return HttpResponseRedirect(checkout_session.url)
    except Exception as e:
        logger.error(f"Error creating Stripe checkout session for user {request.user.id}: {str(e)}")
        messages.error(request, f'Could not initiate payment: {str(e)}')
        # Redirect back to the page they came from (payment required or account)
        return redirect('accounts:payment_required')

# --- Stripe Webhook Handler --- # 
@csrf_exempt 
@require_POST 
def stripe_webhook_view(request):
    """Listens for events from Stripe."""
    payload = request.body
    sig_header = request.META.get('HTTP_STRIPE_SIGNATURE')
    # Get webhook secret from settings (MUST be set in production)
    endpoint_secret = getattr(settings, 'STRIPE_WEBHOOK_SECRET', None) 

    if not endpoint_secret:
        logger.error("Stripe webhook secret is not configured.")
        return HttpResponse(status=500) # Internal Server Error

    event = None

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, endpoint_secret
        )
        logger.info(f"Stripe webhook event received: {event['type']}")
    except ValueError as e:
        # Invalid payload
        logger.error(f"Stripe webhook error (Invalid payload): {e}")
        return HttpResponse(status=400)
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        logger.error(f"Stripe webhook error (Invalid signature): {e}")
        return HttpResponse(status=400)
    except Exception as e:
        # Catch any other unexpected errors during event construction
        logger.error(f"Stripe webhook - Unexpected error during event construction: {e}", exc_info=True)
        return HttpResponse(status=500)

    # Handle both checkout.session.completed event (for one-time payments)
    # and payment_intent.succeeded events (sometimes sent for one-time payments)
    if event['type'] == 'checkout.session.completed' or event['type'] == 'payment_intent.succeeded':
        if event['type'] == 'checkout.session.completed':
            session = event['data']['object']
            # Extract user_id from metadata
            user_id = session.metadata.get('user_id')
            # Check payment status
            payment_status = session.payment_status
            logger.info(f"Processing checkout.session.completed for session ID: {session.id}")
            logger.debug(f"Session details: Status={session.status}, PaymentStatus={payment_status}, Metadata={session.metadata}")
        else:  # payment_intent.succeeded
            payment_intent = event['data']['object'] 
            # Extract user_id from metadata
            user_id = payment_intent.metadata.get('user_id')
            # For payment_intent.succeeded, if it completes, it's paid
            payment_status = 'paid'
            logger.info(f"Processing payment_intent.succeeded for intent ID: {payment_intent.id}")
            logger.debug(f"Payment Intent details: Status={payment_intent.status}, Metadata={payment_intent.metadata}")
            
        # Process the payment for both event types
        if payment_status == 'paid':
            if user_id:
                logger.info(f"Payment completed for user_id: {user_id}")
                try:
                    user = User.objects.get(pk=user_id)
                    profile, created = UserProfile.objects.get_or_create(user=user)
                    if created:
                        logger.warning(f"UserProfile created via webhook for user {user.username}")
                    
                    # Grant paid access
                    profile.has_paid_access = True
                    # For one-time payments, we can set it to never expire by leaving paid_access_expiry_date as None
                    # If you want an expiration date, uncomment and modify:
                    # from datetime import timedelta
                    # profile.paid_access_expiry_date = timezone.now() + timedelta(days=365)  # 1 year access
                    profile.save()
                    logger.info(f"Updated UserProfile for user {user.username}: has_paid_access=True")
                    
                except User.DoesNotExist:
                    logger.error(f"User with ID {user_id} not found from webhook metadata.")
                except Exception as e:
                    logger.error(f"Error updating UserProfile for user_id {user_id}: {e}", exc_info=True)
                    return HttpResponse(status=500) 
            else:
                logger.error("user_id not found in webhook metadata.")
        else:
            logger.warning(f"Payment not completed. Payment status is '{payment_status}' (expected 'paid'). No profile update.")

    # Acknowledge receipt to Stripe
    return HttpResponse(status=200)

class CustomPasswordChangeView(PasswordChangeView):
    """Custom password change view with enhanced error logging."""
    template_name = 'registration/password_change_form.html'
    success_url = reverse_lazy('password_change_done')
    
    def form_invalid(self, form):
        """Log detailed information about the form errors."""
        logger.debug("Password change form invalid")
        
        # Log non-field errors
        if form.non_field_errors():
            logger.debug(f"Non-field errors: {form.non_field_errors()}")
        
        # Log field errors
        for field_name, errors in form.errors.items():
            logger.debug(f"Field '{field_name}' errors: {errors}")
        
        return super().form_invalid(form)
    
    def form_valid(self, form):
        logger.debug("Password change form valid")
        return super().form_valid(form)
