# GCopy

This is a Python script to download files from Google Cloud Storage. 

It supports optional multithreading by using `-m` flag, with thread limits set in `~/.boto` config file.

### Requirements
* Python3
* Google Cloud Storage SDK (`google-cloud-storage`)
* Private key for access to a storage bucket

### Usage
```commandline
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
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
