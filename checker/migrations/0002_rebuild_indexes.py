from django.db import migrations

class Migration(migrations.Migration):

    dependencies = [
        ('checker', '0001_initial'),  # Replace with your previous migration
    ]

    operations = [
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_cmu_id ON checker_component (cmu_id);",
            "DROP INDEX IF EXISTS idx_component_cmu_id;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_company_name ON checker_component (company_name);",
            "DROP INDEX IF EXISTS idx_component_company_name;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_technology ON checker_component (technology);",
            "DROP INDEX IF EXISTS idx_component_technology;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_delivery_year ON checker_component (delivery_year);",
            "DROP INDEX IF EXISTS idx_component_delivery_year;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS idx_component_location ON checker_component (location);",
            "DROP INDEX IF EXISTS idx_component_location;"
        ),
    ]