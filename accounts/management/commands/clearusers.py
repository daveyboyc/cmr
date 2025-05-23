from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

class Command(BaseCommand):
    help = 'Clears all users except superusers'

    def add_arguments(self, parser):
        parser.add_argument(
            '--keep-admin',
            action='store_true',
            dest='keep_admin',
            default=True,
            help='Keep admin users (is_superuser=True)',
        )
        parser.add_argument(
            '--email',
            dest='email',
            help='Delete a specific user by email',
        )

    def handle(self, *args, **options):
        keep_admin = options['keep_admin']
        email = options['email']
        
        if email:
            try:
                user = User.objects.get(email=email)
                username = user.username
                user.delete()
                self.stdout.write(self.style.SUCCESS(f'Successfully deleted user: {username}'))
            except User.DoesNotExist:
                self.stdout.write(self.style.WARNING(f'User with email {email} not found'))
            return
            
        # Delete users based on filters
        if keep_admin:
            deleted, _ = User.objects.filter(is_superuser=False).delete()
            self.stdout.write(self.style.SUCCESS(f'Successfully deleted {deleted} non-admin users'))
        else:
            deleted, _ = User.objects.all().delete()
            self.stdout.write(self.style.SUCCESS(f'Successfully deleted {deleted} users (including admins)')) 