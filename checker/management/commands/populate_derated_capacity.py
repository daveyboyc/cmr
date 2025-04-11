import logging
from django.core.management.base import BaseCommand
from django.db import transaction
from checker.models import Component

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Populates the derated_capacity_mw field from additional_data["De-Rated Capacity"] for existing components.'

    def handle(self, *args, **options):
        self.stdout.write(self.style.NOTICE('Starting population of derated_capacity_mw field...'))
        
        updated_count = 0
        skipped_count = 0
        error_count = 0
        total_count = Component.objects.count()
        batch_size = 500 # Process in batches to manage memory

        # Use iterator to process components in batches
        component_iterator = Component.objects.only('id', 'additional_data').iterator(chunk_size=batch_size)

        components_to_update = []

        try:
            for i, component in enumerate(component_iterator):
                if component.additional_data and isinstance(component.additional_data, dict):
                    capacity_str = component.additional_data.get("De-Rated Capacity")
                    
                    if capacity_str is not None:
                        try:
                            capacity_float = float(capacity_str)
                            # Only update if the value is different or null
                            if component.derated_capacity_mw != capacity_float:
                                component.derated_capacity_mw = capacity_float
                                components_to_update.append(component)
                                updated_count += 1
                            else:
                                skipped_count += 1 # Already has the correct value
                        except (ValueError, TypeError):
                            # Handle cases where conversion is not possible (e.g., non-numeric string)
                            # Optionally log these cases
                            # logger.warning(f"Component {component.id}: Could not convert '{capacity_str}' to float.")
                            if component.derated_capacity_mw is not None:
                                component.derated_capacity_mw = None # Set to null if value is invalid
                                components_to_update.append(component)
                                error_count += 1
                            else:
                                skipped_count += 1 # Value is invalid and field is already null
                    else:
                        # De-Rated Capacity key not found in additional_data
                        if component.derated_capacity_mw is not None:
                             component.derated_capacity_mw = None # Set to null if key is missing
                             components_to_update.append(component)
                             updated_count += 1 # Count as update if field was not null
                        else:
                             skipped_count += 1 # Key missing and field already null
                else:
                    # No additional_data or it's not a dictionary
                    if component.derated_capacity_mw is not None:
                         component.derated_capacity_mw = None # Set to null
                         components_to_update.append(component)
                         updated_count += 1 # Count as update if field was not null
                    else:
                         skipped_count += 1 # No data and field already null

                # Update in batches
                if len(components_to_update) >= batch_size:
                    with transaction.atomic():
                        Component.objects.bulk_update(components_to_update, ['derated_capacity_mw'])
                    self.stdout.write(f'Processed {i + 1}/{total_count} components...')
                    components_to_update = [] # Reset batch

            # Update any remaining components
            if components_to_update:
                with transaction.atomic():
                    Component.objects.bulk_update(components_to_update, ['derated_capacity_mw'])
            
            self.stdout.write(self.style.SUCCESS(
                f'Successfully processed {total_count} components. '
                f'Updated: {updated_count}, Skipped (no change needed): {skipped_count}, Errors (set to null): {error_count}'
            ))

        except Exception as e:
            logger.exception("An error occurred during population.")
            self.stdout.write(self.style.ERROR(f'An error occurred: {e}')) 