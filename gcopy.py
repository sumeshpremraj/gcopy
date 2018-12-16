#!/usr/bin/env python
import os
import errno
import sys
import argparse
import configparser
from threading import Thread
from queue import Queue
from google.cloud import storage
from google.api_core.exceptions import NotFound

class GCopy(object):
    def __init__(self):
        self.parallel_process_count = 0
        self.num_threads = 0


    def parse_config(self):
        try:
            filename = os.path.expanduser('~/.boto')
            config = configparser.ConfigParser()
            res = config.read(filename)

            if not len(res):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)

            self.num_threads = int(config['default']['parallel_thread_count'])
            self.parallel_process_count = config['default']['parallel_process_count']

        except configparser.NoSectionError as e:
            print('default section not found')
        except configparser.NoOptionError as e:
            print('parallel thread/process count option not found')
        except Exception as e:
            print(str(e))

    def transfer_file(self, q):
        # Check if this is a download or upload
        while not q.empty():
            info = q.get()
            dest = info['dest']
            blob = info['blob']
            download = info['download']

            if download:
                self.create_dir(dest + blob.name)
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
                    q.task_done()

            else:
                # Upload
                # Implement if required
                pass

    def create_dir(self, path):
        # Create only if path doesn't exist
        dirs = os.path.dirname(path)
        if not os.path.exists(dirs):
            # makedirs creates nested directories for given path
            print("Creating " + dirs)
            os.makedirs(dirs)

    def copy_full(self, source, dest, download, parallel_thread_count=None, parallel_process_count=None):
        if download:
            if not os.path.exists(dest):
                print(dest + " does not exist, please create it first")
                sys.exit(1)

            # for path = gs://online-infra-engineer-test/mydir/a/b/
            # prefix = mydir/a/b/
            prefix = '/'.join(source[5:].split('/')[1:])
            q = Queue(maxsize=0)

            blobs = bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                print("\nProcessing file " + str(blob.name))
                self.create_dir(dest + blob.name)
                info = {'source': source, 'dest': dest, 'blob': blob, 'download': download}
                q.put(info)
                # self.transfer_file(source, dest, blob, download)

            for i in range(self.num_threads):
                thread = Thread(target=self.transfer_file, args=[q])
                thread.start()

            q.join()

        else:
            # Upload
            pass

        print("\n\033[92m[OK]\033[00m Transfer completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", help="", action="store_true")
    parser.add_argument("source", help="Source URL (gs://xxxx)")
    parser.add_argument("dest", help="Local path to download to (this directory must exist)")
    args = parser.parse_args()

    source = args.source
    dest = args.dest

    if source.startswith("gs://"):
        download = True
    elif dest.startswith("gs://"):
        download = False
    else:
        print("One of source/destination should be a Google Cloud Storage URL")
        sys.exit(1)

    # Add trailing slash to local path, if required
    if dest[-1] != '/':
        dest += '/'

    bucket_name = source.split(':')[1].lstrip('/').split('/')[0]
    print("Bucket: " + bucket_name,)
    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.get_bucket(bucket_name)

    gc = GCopy()
    if args.m:
        gc.parse_config()

    gc.copy_full(source, dest, download)