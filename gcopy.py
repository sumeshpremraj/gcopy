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
        """
        Set thread count as 1 by default
        If `-m` flag is set, parse configs and override this value
        """
        self.num_threads = 1


    def parse_config(self):
        """
        Parse boto config and set maximum number of threads to (process count * thread count)
        Two reasons:
            - Linux implements processes and threads similarly
            - simplify implementation (managing processes AND threads will get complicated, for little benefit in a
            network IO-bound script like this)
        """
        try:
            filename = os.path.expanduser('~/.boto')
            config = configparser.ConfigParser()
            res = config.read(filename)
            self.num_threads = int(config['default']['parallel_thread_count']) * int(config['default']['parallel_process_count'])

        except configparser.NoSectionError as e:
            print('Default section not found')
        except configparser.NoOptionError as e:
            print('Parallel thread/process count option not found')
        except Exception as e:
            print(str(e))

    def transfer_file(self, q):
        """
        Thread-safe method to transfer individual files
        :param q: queue of details for file transfer
        """
        while not q.empty():
            info = q.get()
            dest = info['dest']
            blob = info['blob']
            download = info['download']

            # Check if this is a download or upload
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
                    # Use ANSI escape code to print [ERROR] in red
                    print("\033[91m ERROR \033[00m404: File " + filename + " not found")
                except Exception as e:
                    print("\033[91m ERROR \033[00m404: " + str(e))
                else:
                    print(dest_dir + filename + " completed")
                    q.task_done()

            else:
                # Upload
                # Implement if required
                pass

    def create_dir(self, path):
        """
        Helper method to create `path` if required
        :param path:
        """
        dirs = os.path.dirname(path)
        if not os.path.exists(dirs):
            # makedirs creates nested directories for given path
            print("Creating " + dirs)
            os.makedirs(dirs)

    def copy_full(self, source, dest, download=True):
        """
        Main method to parse details and initialize threads for file transfer
        :param source: remote path
        :param dest: local path, must exist
        :param download: optional parameter to use if we implement upload functionality too
        """
        if download:
            if not os.path.exists(dest):
                print(dest + " does not exist, please create it first")
                sys.exit(1)

            """
            Extract prefix for use as a filter in list_blobs()

            For path = gs://online-infra-engineer-test/mydir/a/b/
            We get prefix = mydir/a/b/
            """
            prefix = '/'.join(source[5:].split('/')[1:])
            q = Queue(maxsize=0)

            # Setting count instead of using q.qsize() because it may not be thread-safe
            count = 0

            blobs = bucket.list_blobs(prefix=prefix)

            # Parse blob list and store details into queue
            for blob in blobs:
                print("Processing file " + str(blob.name))
                info = {'source': source, 'dest': dest, 'blob': blob, 'download': download}
                q.put(info)
                count += 1

            # Start threads for file transfer
            for i in range(min(self.num_threads, count)):
                thread = Thread(target=self.transfer_file, args=[q])
                thread.start()
            q.join()

        else:
            # Upload
            pass

        print("\033[92m[OK]\033[00m Transfer completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", help="Use multithreading (can worsen performance on slow networks, use with caution)", action="store_true")
    parser.add_argument("source", help="Source URL (gs://xxxx)")
    parser.add_argument("dest", help="Local path to download to (this directory must exist)")
    args = parser.parse_args()

    source = args.source
    dest = args.dest

    # Simple validation. Better implementation would be to use regex here
    if source.startswith("gs://"):
        download = True
    elif dest.startswith("gs://"):
        download = False
    else:
        print("One of source/destination should be a Google Cloud Storage URL")
        sys.exit(1)

    # Add trailing slash, if required
    if dest[-1] != '/':
        dest += '/'
    if source[-1] != '/':
        source += '/'

    bucket_name = source[5:].split('/')[0]
    print("Bucket: " + bucket_name,)
    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.get_bucket(bucket_name)

    gc = GCopy()
    if args.m:
        gc.parse_config()

    gc.copy_full(source, dest)