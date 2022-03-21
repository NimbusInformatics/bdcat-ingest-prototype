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

from bdcat_ingest import generate_dict_from_file_directory
from bdcat_ingest import get_manifest_file_pointer_for_directory
from bdcat_ingest import update_manifest_file

out_file_path = ''
out_file = ''
cloud_bucket_name = ''

def main():
	args = parse_args()
	print('Script running on', sys.platform, 'with', os.cpu_count(), 'cpus')

	# process file
	od = OrderedDict()
	od = generate_dict_from_file_directory(args.directory, args.study_id, args.consent_group, None)
	global out_file
	out_file = get_manifest_file_pointer_for_directory(args.directory)	
	update_manifest_file(out_file, od)				
	out_file.close()
	print("Done. Manifest located at", out_file.name)

def parse_args():
	parser = argparse.ArgumentParser(description='Generate manifest TSV file for file directory.')
	parser.add_argument('--directory', required=True , help='directory')
	parser.add_argument('--study_id', type=str, default='', help='study_id')
	parser.add_argument('--consent_group', type=str, default='', help='consent group')
			
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
