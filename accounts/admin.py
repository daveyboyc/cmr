from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin
from .models import UserProfile, RegistrationEmailRecord
from django.utils.html import format_html
from django.urls import reverse

# Register your models here.
class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False
    verbose_name_plural = 'User Profiles'

# Extend the default UserAdmin
class CustomUserAdmin(UserAdmin):
    inlines = (UserProfileInline,)
    list_display = ('username', 'email', 'first_name', 'last_name', 'date_joined', 'is_active', 'is_staff')
    list_filter = ('is_active', 'is_staff', 'date_joined')
    search_fields = ('username', 'email', 'first_name', 'last_name')
    ordering = ('-date_joined',)
    
    # Add a method to show pending activations
    def get_fieldsets(self, request, obj=None):
        fieldsets = super().get_fieldsets(request, obj)
        if request and not obj:
            # Show a list of pending activations in the add form
            from django.utils import timezone
            pending_users = User.objects.filter(is_active=False).order_by('-date_joined')
            if pending_users.exists():
                self.message_user(request, f"{pending_users.count()} users pending activation")
        return fieldsets

# Re-register UserAdmin
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)

# Register UserProfile directly too
@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'has_paid_access', 'paid_access_expiry_date', 'is_paid_access_active')
    list_filter = ('has_paid_access',)
    search_fields = ('user__username', 'user__email')
    readonly_fields = ('is_paid_access_active',)

# Create a custom filter for the User admin to easily see pending activations
class PendingActivationFilter(admin.SimpleListFilter):
    title = 'activation status'
    parameter_name = 'pending'
    
    def lookups(self, request, model_admin):
        return (
            ('pending', 'Pending Activation'),
            ('activated', 'Activated'),
        )
    
    def queryset(self, request, queryset):
        if self.value() == 'pending':
            return queryset.filter(is_active=False)
        if self.value() == 'activated':
            return queryset.filter(is_active=True)
        return queryset

# Add the filter to the CustomUserAdmin
CustomUserAdmin.list_filter = ('is_active', 'is_staff', 'date_joined', PendingActivationFilter)

# Register the RegistrationEmailRecord model
@admin.register(RegistrationEmailRecord)
class RegistrationEmailRecordAdmin(admin.ModelAdmin):
    list_display = ('email', 'timestamp', 'status_display', 'action_buttons')
    list_filter = ('user_created', 'user_activated', 'timestamp')
    search_fields = ('email',)
    readonly_fields = ('email', 'timestamp', 'activation_link', 'user_created', 'user_activated', 'error_message')
    
    def status_display(self, obj):
        if obj.user_activated:
            return format_html('<span style="color: green;">Activated</span>')
        elif obj.user_created:
            return format_html('<span style="color: orange;">Created (Not Activated)</span>')
        else:
            return format_html('<span style="color: red;">Pending (No User)</span>')
    status_display.short_description = 'Status'
    
    def action_buttons(self, obj):
        # Add actions based on status
        if not obj.user_created and not obj.user_activated:
            # No user exists - offer to create one manually
            user_exists = User.objects.filter(email=obj.email).exists()
            if not user_exists:
                return format_html(
                    '<a href="{}?email={}" class="button">Resend Activation Email</a>',
                    reverse('admin:auth_user_add'),
                    obj.email
                )
            else:
                return "Email exists but not linked"
        elif obj.user_created and not obj.user_activated:
            # User exists but not activated
            try:
                user = User.objects.get(email=obj.email, is_active=False)
                return format_html(
                    '<a href="{}" class="button">View User</a>',
                    reverse('admin:auth_user_change', args=[user.pk])
                )
            except User.DoesNotExist:
                return "User not found"
        else:
            # User is activated
            try:
                user = User.objects.get(email=obj.email, is_active=True)
                return format_html(
                    '<a href="{}" class="button">View User</a>',
                    reverse('admin:auth_user_change', args=[user.pk])
                )
            except User.DoesNotExist:
                return "User not found"
    action_buttons.short_description = 'Actions'
