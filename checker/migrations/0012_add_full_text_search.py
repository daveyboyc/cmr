from django.db import migrations, models
import django.contrib.postgres.indexes
import django.contrib.postgres.search

class Migration(migrations.Migration):

    dependencies = [
        ('checker', '0011_component_county_outward_idx'),
    ]

    operations = [
        # Add search vector fields to the Component model
        migrations.AddField(
            model_name='component',
            name='search_vector',
            field=django.contrib.postgres.search.SearchVectorField(null=True),
        ),
        
        # Add GIN index for the search vector
        migrations.AddIndex(
            model_name='component',
            index=django.contrib.postgres.indexes.GinIndex(fields=['search_vector'], name='component_search_idx'),
        ),
        
        # Create updateable trigger function
        migrations.RunSQL(
            sql="""
            CREATE OR REPLACE FUNCTION component_search_vector_update() RETURNS trigger AS $$
            BEGIN
                NEW.search_vector = 
                    setweight(to_tsvector('english', COALESCE(NEW.company_name, '')), 'A') ||
                    setweight(to_tsvector('english', COALESCE(NEW.location, '')), 'A') ||
                    setweight(to_tsvector('english', COALESCE(NEW.county, '')), 'B') ||
                    setweight(to_tsvector('english', COALESCE(NEW.outward_code, '')), 'B') ||
                    setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'C') ||
                    setweight(to_tsvector('english', COALESCE(NEW.technology, '')), 'C') ||
                    setweight(to_tsvector('english', COALESCE(NEW.cmu_id, '')), 'D') ||
                    setweight(to_tsvector('english', COALESCE(NEW.auction_name, '')), 'D') ||
                    setweight(to_tsvector('english', COALESCE(NEW.delivery_year, '')), 'D');
                RETURN NEW;
            END
            $$ LANGUAGE plpgsql;
            """,
            reverse_sql="DROP FUNCTION IF EXISTS component_search_vector_update() CASCADE;",
        ),
        
        # Create trigger for automatic updates
        migrations.RunSQL(
            sql="""
            DROP TRIGGER IF EXISTS component_search_trigger ON checker_component;
            CREATE TRIGGER component_search_trigger
            BEFORE INSERT OR UPDATE
            ON checker_component
            FOR EACH ROW
            EXECUTE FUNCTION component_search_vector_update();
            """,
            reverse_sql="DROP TRIGGER IF EXISTS component_search_trigger ON checker_component;",
        ),
        
        # Update existing records with search vectors
        migrations.RunSQL(
            sql="""
            UPDATE checker_component
            SET search_vector = 
                setweight(to_tsvector('english', COALESCE(company_name, '')), 'A') ||
                setweight(to_tsvector('english', COALESCE(location, '')), 'A') ||
                setweight(to_tsvector('english', COALESCE(county, '')), 'B') ||
                setweight(to_tsvector('english', COALESCE(outward_code, '')), 'B') ||
                setweight(to_tsvector('english', COALESCE(description, '')), 'C') ||
                setweight(to_tsvector('english', COALESCE(technology, '')), 'C') ||
                setweight(to_tsvector('english', COALESCE(cmu_id, '')), 'D') ||
                setweight(to_tsvector('english', COALESCE(auction_name, '')), 'D') ||
                setweight(to_tsvector('english', COALESCE(delivery_year, '')), 'D')
            """,
            reverse_sql="",
        ),
    ]