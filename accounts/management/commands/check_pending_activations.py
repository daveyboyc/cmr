from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import User
from django.utils import timezone
from django.conf import settings
from django.core.mail import send_mail
from django.utils.http import urlsafe_base64_encode
from django.utils.encoding import force_bytes
from django.contrib.auth.tokens import default_token_generator
from django.urls import reverse

class Command(BaseCommand):
    help = 'Shows and manages users who have registered but not activated their accounts'

    def add_arguments(self, parser):
        parser.add_argument(
            '--remind',
            action='store_true',
            help='Send reminder emails to users who have not activated their accounts',
        )
        parser.add_argument(
            '--days',
            type=int,
            default=7,
            help='Send reminders to users who registered more than X days ago (default: 7)',
        )
        parser.add_argument(
            '--activate',
            type=str,
            help='Manually activate a user by email address',
        )
        parser.add_argument(
            '--list-all',
            action='store_true',
            help='List all pending activations regardless of age',
        )

    def handle(self, *args, **options):
        # Get pending users
        pending_users = User.objects.filter(is_active=False).order_by('-date_joined')
        
        if pending_users.count() == 0:
            self.stdout.write(self.style.SUCCESS('No pending activations found.'))
            return
            
        # List pending activations
        if options['list_all']:
            self.stdout.write(self.style.NOTICE(f'Found {pending_users.count()} pending activations:'))
            for user in pending_users:
                days_pending = (timezone.now() - user.date_joined).days
                self.stdout.write(f"- {user.email} (pending for {days_pending} days, registered on {user.date_joined.strftime('%Y-%m-%d')})")
            return
            
        # Handle specific email activation
        if options['activate']:
            email = options['activate']
            try:
                user = User.objects.get(username=email, is_active=False)
                user.is_active = True
                user.save()
                self.stdout.write(self.style.SUCCESS(f'Successfully activated user: {email}'))
                
                # Send notification to admin
                admin_subject = f'User Account Manually Activated: {user.email}'
                admin_message = f'The user with email: {user.email} has been manually activated by an administrator.'
                try:
                    send_mail(
                        admin_subject,
                        admin_message,
                        settings.DEFAULT_FROM_EMAIL,
                        [settings.DEFAULT_FROM_EMAIL],
                        fail_silently=True
                    )
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f'Failed to send admin notification: {e}'))
                    
            except User.DoesNotExist:
                self.stdout.write(self.style.ERROR(f'User with email {email} not found or already active'))
            return
            
        # Send reminders for old pending activations
        if options['remind']:
            days_threshold = options['days']
            cutoff_date = timezone.now() - timezone.timedelta(days=days_threshold)
            old_pending = pending_users.filter(date_joined__lt=cutoff_date)
            
            if old_pending.count() == 0:
                self.stdout.write(self.style.SUCCESS(f'No pending activations older than {days_threshold} days.'))
                return
                
            self.stdout.write(self.style.NOTICE(f'Found {old_pending.count()} pending activations older than {days_threshold} days:'))
            
            for user in old_pending:
                days_pending = (timezone.now() - user.date_joined).days
                self.stdout.write(f"- {user.email} (pending for {days_pending} days)")
                
                # Generate activation link
                uid = urlsafe_base64_encode(force_bytes(user.pk))
                token = default_token_generator.make_token(user)
                activation_url = reverse('accounts:activate', kwargs={'uidb64': uid, 'token': token})
                activation_link = f"{settings.SITE_SCHEME}://{settings.SITE_DOMAIN}{activation_url}"
                
                # Send reminder email
                subject = 'Reminder: Activate Your Account'
                message = f'''
Hello,

You recently registered for an account on the Capacity Market Checker, but your account is not yet activated.
Please activate your account by clicking the link below:

{activation_link}

If you did not request this account, you can safely ignore this email.

Regards,
The Capacity Market Checker Team
                '''
                
                try:
                    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, [user.email])
                    self.stdout.write(self.style.SUCCESS(f'  Reminder sent to {user.email}'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'  Failed to send reminder to {user.email}: {e}'))
            
            return
            
        # Default: just show count
        self.stdout.write(self.style.NOTICE(f'Found {pending_users.count()} pending activations.'))
        self.stdout.write(f'Run "python manage.py check_pending_activations --list-all" to see details.')
        self.stdout.write(f'Run "python manage.py check_pending_activations --remind" to send reminders to users.') 