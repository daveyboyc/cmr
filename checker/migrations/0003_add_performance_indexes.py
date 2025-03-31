from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('checker', '0002_rebuild_indexes'),  # Make sure to use the actual latest migration
    ]

    operations = [
        # Add additional indexes to improve search performance
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_company_name_lower ON checker_component (LOWER(company_name));",
            "DROP INDEX IF EXISTS idx_component_company_name_lower;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_delivery_year_auction ON checker_component (delivery_year, auction_name);",
            "DROP INDEX IF EXISTS idx_component_delivery_year_auction;"
        ),
        # Add a functional index for case-insensitive text search
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_description_pattern ON checker_component USING gin(description gin_trgm_ops);",
            "DROP INDEX IF EXISTS idx_component_description_pattern;"
        ),
        # If the above gin index fails due to missing extension, use this instead:
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_description_lower ON checker_component (LOWER(description));",
            "DROP INDEX IF EXISTS idx_component_description_lower;"
        ),
    ]