#!/usr/bin/env python3

import argparse
import sys
from collections import OrderedDict 

from bdcat_ingest import update_manifest_file
from bdcat_ingest import generate_dict_from_input_manifest_file
from bdcat_ingest import update_md5s_from_file
out_file_path = ''
out_file = ''
cloud_bucket_name = ''

def main():
	args = parse_args()
	# process file
	od = generate_dict_from_input_manifest_file(args.tsv, None)
	update_md5s_from_file(od, args.md5_file)
	update_manifest_file(args.out_file, od)				
	args.out_file.close()
	args.tsv.close()
	print("Done. Receipt manifest located at", args.out_file.name)

def parse_args():
	parser = argparse.ArgumentParser(description='Update TSV file with md5ums.')
	parser.add_argument('--tsv', required=True, type=argparse.FileType('r'), help='tsv file')
	parser.add_argument('--out_file', required=True, type=argparse.FileType('wt'), help='out file')
	parser.add_argument('--md5_file', required=True, type=argparse.FileType('r'), help='md5sum file')
				
	args = parser.parse_args()
	if (len(sys.argv) == 0):
		parser.print_help()		
	return args

       
if __name__ == '__main__': 
    main()
