# GCopy

This is a Python script to download files from Google Cloud Storage. 

It supports optional multithreading by using `-m` flag, with thread limits set in `~/.boto` config file.

### Requirements
* Python3
* Google Cloud Storage SDK (`google-cloud-storage`)
* Private key for access to a storage bucket (service_account.json at root of project dir, git is configured to ignore this file so you don't accidentally commit it)

### Usage
```commandline
$ ./build.sh
Setting up virtual env...
Activating venv
Installing dependencies
Done
$ ls service_account.json
service_account.json
$ cat ~/.boto
[default]
parallel_thread_count = 10
parallel_process_count = 5
$ ./gcopy -h
usage: gcopy.py [-h] [-m] source dest

positional arguments:
  source      Source URL (gs://xxxx)
  dest        Local path to download to (this directory must exist)

optional arguments:
  -h, --help  show this help message and exit
  -m          Use multithreading (can worsen performance on slow networks, use
              with caution)
```
