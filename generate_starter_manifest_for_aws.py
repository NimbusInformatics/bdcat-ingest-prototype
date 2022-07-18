#!/usr/bin/env python3

# This script generates a TSV manifest file with file information, but does NOT include
# md5sum populated. This would need to be done outside this script.

import argparse
import signal
import sys
import os
from collections import OrderedDict 

from bdcat_ingest import generate_dict_from_s3_bucket
from bdcat_ingest import get_starter_manifest_file_pointer_for_bucket
from bdcat_ingest import update_manifest_file

out_file_path = ''
out_file = ''
cloud_bucket_name = ''

def main():
	args = parse_args()
	print('Script running on', sys.platform, 'with', os.cpu_count(), 'cpus')

	global out_file
	out_file = get_starter_manifest_file_pointer_for_bucket(args.bucket)	
	# process file
	od = OrderedDict()
	od = generate_dict_from_s3_bucket(args.bucket, '', '')
	update_manifest_file(out_file, od)				
	out_file.close()
	print("Done. Receipt manifest located at", out_file.name)

def parse_args():
	parser = argparse.ArgumentParser(description='Generate Starter TSV file for S3 Bucket.')
	parser.add_argument('--bucket', required=True , help='s3 bucket name')
			
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
