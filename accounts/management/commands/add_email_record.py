from django.core.management.base import BaseCommand
from accounts.models import RegistrationEmailRecord
from django.utils import timezone
from django.core.validators import validate_email
from django.core.exceptions import ValidationError

class Command(BaseCommand):
    help = 'Manually add a registration email record for tracking'

    def add_arguments(self, parser):
        parser.add_argument('email', type=str, help='Email address to add to tracking')
        parser.add_argument(
            '--activation-link',
            type=str,
            dest='activation_link',
            help='Optional activation link that was sent',
        )
        parser.add_argument(
            '--notes',
            type=str,
            help='Optional notes about this email record',
        )

    def handle(self, *args, **options):
        email = options['email']
        
        # Validate email
        try:
            validate_email(email)
        except ValidationError:
            self.stdout.write(self.style.ERROR(f'Invalid email address: {email}'))
            return
            
        # Check if record already exists
        existing_records = RegistrationEmailRecord.objects.filter(email=email)
        if existing_records.exists():
            self.stdout.write(self.style.WARNING(f'Records already exist for email {email}:'))
            for record in existing_records:
                status = "activated" if record.user_activated else ("created" if record.user_created else "pending")
                self.stdout.write(f"- {record.timestamp.strftime('%Y-%m-%d %H:%M')} - Status: {status}")
            
            confirm = input('Do you want to add another record anyway? (y/N): ')
            if confirm.lower() != 'y':
                self.stdout.write(self.style.WARNING('Operation cancelled.'))
                return
        
        # Create record
        record = RegistrationEmailRecord(
            email=email,
            activation_link=options.get('activation_link', ''),
            error_message=options.get('notes', 'Manually added record'),
        )
        record.save()
        
        self.stdout.write(self.style.SUCCESS(f'Successfully added record for email: {email}'))
        self.stdout.write('You can now see this in the admin panel under Registration Email Records.') 