#!/usr/bin/env python3

import argparse
#import subprocess
#import datetime
#import hashlib
#import fileinput
#import csv
import signal
import sys
#import time
#import io
import os
#import tempfile
#import uuid
#from os import access, R_OK
#from os.path import isfile, basename
from collections import OrderedDict 
#from urllib.parse import urlparse

from bdcat_ingest import get_receipt_manifest_file_pointer_for_bucket
from bdcat_ingest import update_manifest_file
from bdcat_ingest import calculate_md5sum_for_cloud_paths_threaded
from bdcat_ingest import upload_manifest_file_to_s3_bucket
from bdcat_ingest import generate_dict_from_input_manifest_file
from bdcat_ingest import generate_ordered_dict_for_updated_files
from bdcat_ingest import update_metadata_for_s3_keys

out_file_path = ''
out_file = ''
cloud_bucket_name = ''

def main():
	args = parse_args()
	print('Script running on', sys.platform, 'with', os.cpu_count(), 'cpus')

	# process file
	od = generate_dict_from_input_manifest_file(args.tsv, None)
	updated_od = generate_ordered_dict_for_updated_files(od)
	calculate_md5sum_for_cloud_paths_threaded(updated_od, args.checksum_threads, )
	update_metadata_for_s3_keys(updated_od)
	global out_file
	out_file = get_receipt_manifest_file_pointer_for_bucket(args.bucket)	
	update_manifest_file(out_file, od)				
	out_file.close()
	upload_manifest_file_to_s3_bucket(out_file.name, args.bucket)
	print("Done. Receipt manifest located at", out_file.name)

def parse_args():
	parser = argparse.ArgumentParser(description='Update TSV file for S3 Bucket.')
	parser.add_argument('--bucket', required=True , help='s3 bucket name')
	parser.add_argument('--tsv', required=True, type=argparse.FileType('r'), help='tsv file')
	parser.add_argument('--checksum_threads', type=int, default=os.cpu_count(), help='number of concurrent checksum threads (default: number of CPUs on machine)')
			
	args = parser.parse_args()
	if (len(sys.argv) == 0):
		parser.print_help()		
	return args

def exit_and_write_manifest_file(signum, frame):
	print ("Detected Quit. Please use the resume manifest file '", out_file_path, "'to resume your job.", sep ='')
	if (out_file):
		out_file.close()
	sys.exit(1)
       
if __name__ == '__main__':
    signal.signal(signal.SIGINT, exit_and_write_manifest_file)
    signal.signal(signal.SIGTERM, exit_and_write_manifest_file)    
    signal.signal(signal.SIGHUP, exit_and_write_manifest_file)    
    main()
