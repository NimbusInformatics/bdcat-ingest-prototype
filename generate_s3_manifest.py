#!/usr/bin/env python3

import argparse
import signal
import sys
import os

from bdcat_ingest import update_metadata_for_s3_keys
from bdcat_ingest import get_receipt_manifest_file_pointer_for_bucket
from bdcat_ingest import generate_dict_from_input_manifest_file
from bdcat_ingest import update_manifest_file

out_file = ''

def main():
	args = parse_args()
	print('Script running version 2.0 on', sys.platform, 'with', os.cpu_count(), 'cpus')

	od = generate_dict_from_input_manifest_file(args.tsv, None)
	update_metadata_for_s3_keys(od)	
	out_file = get_receipt_manifest_file_pointer_for_bucket(args.s3_bucket)	
	update_manifest_file(out_file, od)
#	upload_manifest_file_to_s3_bucket(out_file.name, upload_gcs_bucket_name)
	print("Process complete. Manifest file located at", out_file.name)	
	exit()
	
# Parse user arguments and confirm valid arguments.

def parse_args():
	parser = argparse.ArgumentParser(description='Generate S3 Manifest')
	parser.add_argument('--tsv', required=True, type=argparse.FileType('r'), help='tsv file')
	parser.add_argument('--s3_bucket', type=str, required=True, help='gcs bucket')
		
	args = parser.parse_args()
	if (len(sys.argv) == 0):
		parser.print_help()
	return args	
	
# If a quit is detected, then this method is called to save state.
#
# adapted from here: https://stackoverflow.com/questions/18114560/python-catch-ctrl-c-command-prompt-really-want-to-quit-y-n-resume-executi/18115530

def exit_and_write_manifest_file(signum, frame):
	if (out_file):
		print ("Detected Quit. Please use the resume manifest file '", str(out_file.name), "'to resume your job.", sep ='')
		out_file.close()
	sys.exit(1)
       
if __name__ == '__main__':
    signal.signal(signal.SIGINT, exit_and_write_manifest_file)
    signal.signal(signal.SIGTERM, exit_and_write_manifest_file)    
    signal.signal(signal.SIGHUP, exit_and_write_manifest_file)    
    main()
