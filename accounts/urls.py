from django.urls import path
from . import views

app_name = 'accounts' # Define an app namespace

urlpatterns = [
    path('register/', views.register, name='register'),
    # Add the activation URL
    path('activate/<uidb64>/<token>/', views.activate, name='activate'),
    # Add missing routes
    path('registration-pending/', views.registration_pending, name='registration_pending'),
    path('activation-failed/', views.activation_failed, name='activation_failed'),
    path('must-register/', views.must_register, name='must_register'),
    # Add Account Page URL
    path('account/', views.account_view, name='account'),
    # Add Payment Required URL
    path('payment-required/', views.payment_required_view, name='payment_required'),
    # Add Payment Initiation URL
    path('initiate-payment/', views.initiate_payment_view, name='initiate_payment'),
    # Add Stripe Webhook URL
    path('stripe-webhook/', views.stripe_webhook_view, name='stripe_webhook'),
    # Custom password change view with better error reporting
    path('password-change/', views.CustomPasswordChangeView.as_view(), name='custom_password_change'),
    # Add other account-related URLs here (login, logout, password reset, etc.) later
] 