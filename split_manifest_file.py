#!/usr/bin/env python3

import argparse
import sys

from bdcat_ingest import assign_guids
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



def write_chunk(file_str, header, part, lines):
	with open('%s.%d' % (file_str, part), 'w') as f_out:
		f_out.write(header)
		f_out.writelines(lines)

def main():
	args = parse_args()

	with open(args.tsv, "r") as f:
		count = 0
		header = f.readline()
		lines = []
		for line in f:
			count += 1
			lines.append(line)
			if count % args.num == 0:
				write_chunk(args.tsv, header, count // args.num, lines)
				lines = []
		# write remainder
		if len(lines) > 0:
			write_chunk(args.tsv, header, (count // args.num) + 1, lines)	


def parse_args():
	parser = argparse.ArgumentParser(description='Split manifest file into smaller files')
	parser.add_argument('--num', required=True , type = int, help='number of lines per file')
	parser.add_argument('--tsv', required=True, help='manifest tsv file')

	args = parser.parse_args()
	if (len(sys.argv) == 0):
		parser.print_help()		
	return args
       
if __name__ == '__main__':
    main()
