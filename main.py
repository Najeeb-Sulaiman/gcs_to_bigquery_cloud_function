import os
import logging
from google.cloud import bigquery
from google.cloud import storage

# Initialize clients
bq_client = bigquery.Client()
storage_client = storage.Client()

# Environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
AUDIT_TABLE_ID = os.getenv("AUDIT_TABLE_ID")


def log_event_to_audit_table(event_metadata):
    """
    Function to log event metadata to BigQuery audit table.
    
    Parameters:
    event_metadata (dict): Metadata to be inserted into the audit log table.

    Performs:
    Insert metadata rows into Bigquery audit table.
    
    """
    audit_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{AUDIT_TABLE_ID}"
    
    # Constructing the rows to insert
    rows_to_insert = [event_metadata]

    # Insert rows into BigQuery audit table
    errors = bq_client.insert_rows_json(audit_table_ref, rows_to_insert)
    if errors:
        logging.error(f"Failed to log event metadata: {errors}")


def load_data_to_bigquery(event, context):
    """
    Triggered by a change to a Cloud Storage bucket. Loads data into BigQuery.

    Parameters:
    event (dict): Event payload containing bucket and file details.
    context (dict): Metadata for the event.

    This function loads a CSV file from Cloud Storage into a BigQuery table.
    """
    bucket_name = event["bucket"]
    file_name = event["name"]
    table_name = file_name.split('.')[0]
    table_name = table_name.replace(' ', '_')

    # Constructinging the BigQuery table ID
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.100_{table_name}"

    # Constructing the URI for the CSV file in GCS
    uri = f"gs://{bucket_name}/{file_name}"

    # Prepare event metadata
    event_metadata = {
        "event_id": context.event_id,
        "timestamp": context.timestamp,
        "event_type": context.event_type,
        "resource_name": context.resource.get("name", ""),
        "bucket_name": bucket_name,
        "file_name": file_name,
        "status": "Pending",
        "error_message": None
    }
    
    logging.info(f"Starting load for {uri} into BigQuery table {table_ref}")

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,  # Skip the header row
        autodetect=True,      # Automatically infer schema
        field_delimiter=",",
        allow_quoted_newlines=True,
        quote_character='"',
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Append data to existing table
    )

    try:
        # Loading data from GCS to BigQuery
        load_job = bq_client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )
        
        # Wait for the job to complete
        load_job.result()
        logging.info(f"Job {load_job.job_id} completed successfully")

        # Get the destination table and log the rows
        table = bq_client.get_table(table_ref)
        logging.info(f"Loaded {table.num_rows} rows into {table_ref}")

         # Check for errors in load job
        if load_job.errors:
            event_metadata["status"] = "Failure"
            event_metadata["error_message"] = str(load_job.errors)
            logging.error(f"Load job errors: {load_job.errors}")
        else:
            event_metadata["status"] = "Success"
            logging.info(f"Successfully loaded {file_name} into {table_ref}")


    except Exception as e:
        event_metadata["status"] = "Failure"
        event_metadata["error_message"] = str(e)
        logging.error(f"Failed to load data from {uri} to BigQuery: {str(e)}")
        raise

    finally:
        # Log event metadata to BigQuery audit table 
        log_event_to_audit_table(event_metadata)