from google.cloud import storage
from datetime import datetime, timedelta, timezone
import os

def download_from_bucket(bucket, source_blob_name, destination_file_path):
    # Get the blob (object) from the bucket
    blob = bucket.blob(source_blob_name)
    
    # Download the file to the specified destination
    blob.download_to_filename(destination_file_path)

def list_from_bucket(bucket_name, folder_name, days, bucket, local_folder):
    # Define the prefix for the folder
    prefix = f"{folder_name}/"  # Adds the trailing slash to specify the folder
    
    # List and count blobs in the specified folder
    blobs = bucket.list_blobs(prefix=prefix)
    count = 0

    # Get the current time and calculate the cutoff time
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)

    # Collect blobs created in the last 'days' days
    recent_blobs = [blob for blob in blobs if blob.time_created >= cutoff_time]

    count = len(recent_blobs)

    # Sort blobs by time_created
    recent_blobs.sort(key=lambda blob: blob.time_created)

    print(f"Files in bucket '{bucket_name}':")
    for blob in recent_blobs:
        print(f"Name: {blob.name}, Created: {blob.time_created}")
        file_name = os.path.basename(blob.name)
        download_from_bucket(bucket, blob.name, local_folder + file_name)

    print(f"Total files: {count}")


# Example usage
bucket_name         = 'dpvale-storage-qa'
folder_tickets_name = 'coupons/pdfs/ticket'
folder_policy_name  = 'coupons/pdfs/policy'
days                = 1

# Initialize the authenticated storage client
client          = storage.Client.from_service_account_json('/datos/sleyvaGCP/dpvaleGCP_Credentials.json')
# Retrieve the bucket
bucket          = client.bucket(bucket_name)
local_folder    = '/datos/dportenis-distribuidoras-api/server/storage/dispersion-client/'

list_from_bucket(bucket_name, folder_tickets_name, days, bucket, local_folder)
list_from_bucket(bucket_name, folder_policy_name, days, bucket, local_folder)