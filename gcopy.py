#!/usr/bin/env python
import os
import sys
from google.cloud import storage
from google.api_core.exceptions import NotFound

class GCopy(object):
    def transfer_file(self, blob, dest, download):
        # Check if this is a download or upload
        print("Switching to " + dest[:dest.rindex('/')])
        os.chdir(dest[:dest.rindex('/')])
        if download:
            print("Downloading file " + blob.name + " to " + dest)
            try:
                blob.download_to_filename(dest)
            except NotFound as e:
                print("File not found")
            except Exception as e:
                print(e)
            else:
                print("Download completed.")

        else:
            # Upload
            print("Uploading file " + blob.name + " to " + dest)
            try:
                blob.upload_from_filename(source)
            except NotFound as e:
                print("File not found")
            except Exception as e:
                print(e)
            else:
                print("Upload completed.")

    def create_dir(self, path):
        # Create only if path doesn't exist
        dirs = path.rindex('/')
        if not os.path.exists(path[:dirs]):
            # makedirs creates nested directories for given path
            print("Creating " + path[:dirs])
            os.makedirs(path[:dirs])

    def copy_full(self, source, dest, download):
        """
        source_dir = '/tmp/sumesh/'
        sumesh
        - a
            - 1.txt
            - b
                -2.txt
        dest_dir = '/tmp/arjita/'
        """
        if not os.path.exists(dest):
            print(dest + " does not exist, please create it first")
            sys.exit(1)

        blobs = bucket.list_blobs()
        for blob in blobs:
            # print(blob.name)
            print("Processing file " + str(blob.name))
            self.create_dir(dest+blob.name)
            self.transfer_file(blob, dest + blob.name, download)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid arguments")
        sys.exit(1)

    source = sys.argv[1]
    dest = sys.argv[2]

    if source.startswith("gs://"):
        full_path = source
        download = True
    elif dest.startswith("gs://"):
        full_path = dest
        download = False
    else:
        print("One of source/destination should be a Google Cloud Storage URL")
        sys.exit(1)

    bucket_name = full_path.split(':')[1].lstrip('/').split('/')[0]
    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.get_bucket(bucket_name)

    gc = GCopy()
    gc.copy_full(source, dest, download)