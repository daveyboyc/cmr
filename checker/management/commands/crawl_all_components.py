# checker/management/commands/crawl_all_components.py
from django.core.management.base import BaseCommand
from checker.views import fetch_components_for_cmu_id, get_cmu_data_from_json, fetch_all_cmu_records, \
    save_component_data_to_json
import time
import os
import json
from django.conf import settings
import pandas as pd
from django.core.cache import cache
import traceback


class Command(BaseCommand):
    help = 'Crawl all components from all known CMU IDs and store them in JSON'

    def add_arguments(self, parser):
        parser.add_argument('--batch', type=int, default=20, help='Number of CMU IDs to process per batch')
        parser.add_argument('--sleep', type=int, default=10, help='Seconds to sleep between batches')
        parser.add_argument('--from-letter', type=str, help='Start crawling from CMU IDs beginning with this letter')
        parser.add_argument('--to-letter', type=str, help='Stop crawling at CMU IDs beginning with this letter')
        parser.add_argument('--max-batches', type=int, default=0, help='Maximum number of batches to process (0 = all)')
        parser.add_argument('--company', type=str, help='Process only CMU IDs for company names containing this text')
        parser.add_argument('--test', action='store_true',
                            help='Test mode - only process 1 CMU ID and verify file access')
        parser.add_argument('--resume', action='store_true', help='Resume crawling, skipping already processed CMU IDs')

    def handle(self, *args, **options):
        batch_size = options['batch']
        sleep_time = options['sleep']
        from_letter = options['from_letter'].upper() if options['from_letter'] else None
        to_letter = options['to_letter'].upper() if options['to_letter'] else None
        max_batches = options['max_batches']
        company_filter = options['company'].lower() if options['company'] else None
        test_mode = options['test']
        resume_mode = options['resume']

        # First test JSON directory access
        json_dir = os.path.join(settings.BASE_DIR, 'json_data')
        os.makedirs(json_dir, exist_ok=True)

        # Test if we can write to the directory
        test_file = os.path.join(json_dir, 'crawler_test.json')
        try:
            with open(test_file, 'w') as f:
                json.dump({"test": "success"}, f)
            self.stdout.write(self.style.SUCCESS('JSON directory is writable'))
            # Clean up test file
            os.remove(test_file)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: Cannot write to JSON directory: {e}'))
            return

        # First ensure we have CMU data
        cmu_data = get_cmu_data_from_json()
        if not cmu_data:
            self.stdout.write('No CMU data found. Fetching all CMU data...')
            try:
                cmu_data, _ = fetch_all_cmu_records(limit=5000)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Failed to fetch CMU data: {e}'))
                self.stdout.write(traceback.format_exc())
                return

        # Convert to DataFrame for easier filtering
        cmu_df = pd.DataFrame(cmu_data)

        # Standardize CMU ID field
        possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
        cmu_id_field = next((field for field in possible_cmu_id_fields if field in cmu_df.columns), None)
        if cmu_id_field:
            cmu_df["CMU ID"] = cmu_df[cmu_id_field].fillna("N/A").astype(str)
        else:
            cmu_df["CMU ID"] = "N/A"

        # Process company name fields
        cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
        cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)

        cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
        cmu_df["Full Name"] = cmu_df.apply(
            lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
            axis=1
        )

        # Create mapping before filtering
        cmu_to_company_mapping = {}
        for _, row in cmu_df.iterrows():
            cmu_id = row.get("CMU ID", "").strip()
            if cmu_id and cmu_id != "N/A":
                cmu_to_company_mapping[cmu_id] = row.get("Full Name", "")

        # Filter by company name if specified
        if company_filter:
            # Apply company filter
            self.stdout.write(f'Filtering for company names containing "{company_filter}"...')
            cmu_df = cmu_df[cmu_df["Full Name"].str.lower().str.contains(company_filter, na=False)]
            if cmu_df.empty:
                self.stdout.write(self.style.ERROR(f'No companies found matching "{company_filter}"'))
                return

            # Show found companies
            unique_companies = cmu_df["Full Name"].unique()
            self.stdout.write(f'Found {len(unique_companies)} matching companies:')
            for company in unique_companies:
                self.stdout.write(f'  - {company}')

        # Extract all CMU IDs
        all_cmu_ids = cmu_df["CMU ID"].dropna().unique().tolist()

        # Filter out invalid CMU IDs (empty or non-string)
        all_cmu_ids = [cmu_id for cmu_id in all_cmu_ids if cmu_id and isinstance(cmu_id, str) and cmu_id != "N/A"]

        # Filter by starting letter if specified
        if from_letter:
            all_cmu_ids = [cmu_id for cmu_id in all_cmu_ids if cmu_id and cmu_id[0].upper() >= from_letter]
        if to_letter:
            all_cmu_ids = [cmu_id for cmu_id in all_cmu_ids if cmu_id and cmu_id[0].upper() <= to_letter]

        # Sort alphabetically for predictable ordering
        all_cmu_ids.sort()

        # If resume mode, check which CMU IDs already have component data
        if resume_mode:
            self.stdout.write('Resume mode active, checking for already processed CMU IDs...')
            already_processed = []
            for cmu_id in all_cmu_ids:
                prefix = cmu_id[0].upper() if cmu_id else "0"
                json_path = os.path.join(json_dir, f'components_{prefix}.json')

                if os.path.exists(json_path):
                    try:
                        with open(json_path, 'r') as f:
                            components_data = json.load(f)
                            if cmu_id in components_data and components_data[cmu_id]:
                                already_processed.append(cmu_id)
                    except Exception:
                        # If there's an error reading the file, assume it's not processed
                        pass

            if already_processed:
                self.stdout.write(f'Found {len(already_processed)} already processed CMU IDs, skipping them')
                all_cmu_ids = [cmu_id for cmu_id in all_cmu_ids if cmu_id not in already_processed]

        total_cmus = len(all_cmu_ids)
        self.stdout.write(f'Found {total_cmus} CMU IDs to process')

        # In test mode, just process one CMU ID
        if test_mode:
            self.stdout.write('TEST MODE: Processing only one CMU ID')
            if all_cmu_ids:
                test_cmu = all_cmu_ids[0]
                all_cmu_ids = [test_cmu]
                self.stdout.write(f'TEST MODE: Using CMU ID {test_cmu}')
            else:
                self.stdout.write(self.style.ERROR('TEST MODE: No CMU IDs found to test'))
                return

        # Cache the full mapping of CMU IDs to company names
        cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 24 * 3600)  # Cache for 24 hours
        self.stdout.write(f'Cached mapping for {len(cmu_to_company_mapping)} CMU IDs')

        # Process in batches
        batches = [all_cmu_ids[i:i + batch_size] for i in range(0, len(all_cmu_ids), batch_size)]

        # Limit batches if specified
        if max_batches > 0:
            batches = batches[:max_batches]

        self.stdout.write(f'Processing {len(batches)} batches of up to {batch_size} CMU IDs each')

        start_time = time.time()
        processed_cmus = 0
        processed_batches = 0
        total_components = 0
        failed_cmus = []

        # Process each batch
        for batch_num, batch in enumerate(batches, 1):
            self.stdout.write(f'Processing batch {batch_num}/{len(batches)}...')
            batch_start = time.time()

            for cmu_id in batch:
                company_name = cmu_to_company_mapping.get(cmu_id, "Unknown")
                try:
                    components, _ = fetch_components_for_cmu_id(cmu_id)

                    # Double-check the components were saved to JSON
                    prefix = cmu_id[0].upper() if cmu_id else "0"
                    json_path = os.path.join(json_dir, f'components_{prefix}.json')

                    # Direct file verification
                    if os.path.exists(json_path):
                        try:
                            with open(json_path, 'r') as f:
                                file_data = json.load(f)
                                if cmu_id not in file_data or not file_data[cmu_id]:
                                    # If the file exists but doesn't have our data, try saving directly
                                    self.stdout.write(f'  WARNING: {cmu_id} not found in JSON file, trying direct save')
                                    save_component_data_to_json(cmu_id, components)
                        except Exception as e:
                            self.stdout.write(
                                self.style.WARNING(f'  WARNING: Error checking JSON file for {cmu_id}: {e}'))
                    else:
                        self.stdout.write(f'  WARNING: JSON file for {cmu_id} not found, trying direct save')
                        save_component_data_to_json(cmu_id, components)

                    processed_cmus += 1
                    total_components += len(components)
                    self.stdout.write(
                        f'  {processed_cmus}/{total_cmus}: {cmu_id} ({company_name}) - Found {len(components)} components')
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'  Error processing {cmu_id}: {e}'))
                    failed_cmus.append(cmu_id)
                    # Print the traceback for debugging
                    self.stdout.write(traceback.format_exc())

            processed_batches += 1
            batch_time = time.time() - batch_start
            self.stdout.write(f'Batch completed in {batch_time:.2f}s')

            # Calculate progress and ETA
            elapsed = time.time() - start_time
            progress = processed_batches / len(batches)
            eta = (elapsed / progress) - elapsed if progress > 0 else 0

            self.stdout.write(f'Progress: {progress:.1%} - ETA: {eta / 60:.1f} minutes')

            # Sleep between batches to avoid overwhelming the API
            if batch_num < len(batches):
                self.stdout.write(f'Sleeping for {sleep_time} seconds...')
                time.sleep(sleep_time)

        total_time = time.time() - start_time
        summary = (
            f'Crawl completed in {total_time / 60:.1f} minutes. '
            f'Processed {processed_cmus} CMU IDs with {total_components} total components.'
        )

        if failed_cmus:
            summary += f'\nFailed CMU IDs ({len(failed_cmus)}): {", ".join(failed_cmus[:10])}'
            if len(failed_cmus) > 10:
                summary += f' (and {len(failed_cmus) - 10} more)'

            # Save failed CMUs to a file for retry
            failed_path = os.path.join(json_dir, 'failed_cmus.json')
            try:
                with open(failed_path, 'w') as f:
                    json.dump(failed_cmus, f)
                self.stdout.write(f'Failed CMU IDs saved to {failed_path}')
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error saving failed CMU IDs: {e}'))

        self.stdout.write(self.style.SUCCESS(summary))

