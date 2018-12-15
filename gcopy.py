#!/usr/bin/env python
import os
import sys
from google.cloud import storage
from google.api_core.exceptions import NotFound

class GCopy(object):
    def transfer_file(self, source, dest, blob, download):
        # Check if this is a download or upload
        if download:
            filename = blob.name[blob.name.rindex('/') + 1:]
            dest = dest + blob.name
            dest_dir = dest[:dest.rindex('/')]
            print("Switching to " + dest_dir)
            os.chdir(dest_dir)

            print("Downloading file " + filename + " to " + dest_dir)
            try:
                blob.download_to_filename(dest)
            except NotFound as e:
                # Use ANSI escape code to print ERROR in red
                print("\033[91m ERROR \033[00m404: File " + filename + " not found")
            except Exception as e:
                print("\033[91m ERROR \033[00m404: " + str(e))
            else:
                print("Done.")

        else:
            # Upload

            # filename =
            source = source + filename
            src_dir = source[:source.rindex('/')]
            dest_dir = dest + filename
            print("Switching to " + src_dir)
            os.chdir(src_dir)

            print("Uploading file " + filename + " to " + dest_dir)
            try:
                blob.upload_from_filename(dest)
            except NotFound as e:
                # Use ANSI escape code to print ERROR in red
                print("\033[91mERROR\033[00m404: File " + filename + " not found")
            except Exception as e:
                print("\033[91m[ERROR]\033[00m404 " + str(e))
            else:
                print("Done.")

    def create_dir(self, path):
        # Create only if path doesn't exist
        dirs = os.path.dirname(path)
        if not os.path.exists(dirs):
            # makedirs creates nested directories for given path
            print("Creating " + dirs)
            os.makedirs(dirs)

    def copy_full(self, source, dest, download):
        if not os.path.exists(dest):
            print(dest + " does not exist, please create it first")
            sys.exit(1)

        #print("source = " + source)

        # for path: gs://online-infra-engineer-test/mydir/a/b/
        # extract: mydir/a/b/
        prefix = '/'.join(source[5:].split('/')[1:])
        # print("prefix = " + prefix)
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            print("\nProcessing file " + str(blob.name))
            self.create_dir(dest+blob.name)
            self.transfer_file(source, dest, blob, download)

        print("\n\033[92m[OK]\033[00m Transfer completed.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid arguments")
        # TODO: Print help text
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

    # Add trailing slash to local path, if required
    # TODO: Do this only for local path
    if full_path[-1] != '/':
        full_path += '/'

    bucket_name = full_path.split(':')[1].lstrip('/').split('/')[0]
    print("Bucket: " + bucket_name,)
    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.get_bucket(bucket_name)

    gc = GCopy()
    gc.copy_full(source, dest, download)