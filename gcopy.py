#!/usr/bin/env python
import os
import sys
import re
from google.cloud import storage

class GCopy(object):
    def __init__(self, source, dest, download):
        self.copy_single_file(source, dest, download)

    def copy_single_file(self, source, dest, download):
        # Check if this is a download or upload
        if download:
            source_blob_name = '/'.join(source.split(':')[1].lstrip('/').split('/')[1:])
            print("Downloading file " + source_blob_name + " to " + dest)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(dest)

        else:
            # Upload
            destination_blob_name = '/'.join(dest.split(':')[1].lstrip('/').split('/')[1:])
            print("Uploading file " + source + " to " + destination_blob_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid arguments")
        sys.exit()

    source = sys.argv[1]
    dest = sys.argv[2]

    if source.startswith("gs://"):
        full_path = source
        download = True

    elif dest.startswith("gs://"):
        full_path = dest
        download = False
    else:
        print("Invalid arguments")
        sys.exit()

    bucket_name = full_path.split(':')[1].lstrip('/').split('/')[0]

    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.get_bucket(bucket_name)

    GCopy(source, dest, download)