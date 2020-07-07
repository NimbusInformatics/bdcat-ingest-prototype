#!/usr/bin/env python3

# python process.py --aws --tsv sample.tsv 

import argparse
#import subprocess
#import requests
import hashlib
import csv
import sys
#import os
from os import access, R_OK
from os.path import isfile, basename
from collections import OrderedDict 

# for aws
import logging
import boto3
from botocore.exceptions import ClientError


def main():
	parser = argparse.ArgumentParser(description='Process TSV file.')
	parser.add_argument('--tsv', required=True, type=argparse.FileType('r'), help='tsv file')
	parser.add_argument('--gs', default=False, action='store_true', help='upload to Google Cloud')
	parser.add_argument('--aws', default=False, action='store_true', help='upload to AWS')

	# validate args
	args = parser.parse_args()
	if (len(sys.argv) == 0):
		parser.print_help()
	if (not args.gs and not args.aws):
		print('Error: Either gs or aws needs to be set')
		parser.print_help()
		exit()

	# process file
	od = OrderedDict() 
	reader = csv.DictReader(args.tsv, dialect='excel-tab')			
	for row in reader:
		manifest_row = process_row(od, row)

	checksum_files(od)
	
	if (args.gs):
		upload_to_gcloud(od)
	if (args.aws):
		upload_to_aws(od)
	
	manifest_filepath = args.tsv.name
	if (manifest_filepath.endswith('.tsv')):
		manifest_filepath = manifest_filepath.replace(".tsv", ".manifest.tsv")	
	else:
		manifest_filepath += 'manifest.tsv'
	create_manifest_file(manifest_filepath, od)
			
def process_row(od, row):
	local_file = row['file_local_path']
	od[local_file] = row
	assert isfile(local_file) and access(local_file, R_OK), \
       "File {} doesn't exist or isn't readable".format(local_file)

def checksum_files(od):
	# FIXME
	return

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
# The upload_file method handles large files by splitting them into smaller chunks
# and uploading each chunk in parallel.
# See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html#uploads to
# change default transfer options
 
def upload_to_aws(od): 
	aws_client = boto3.client('s3')

    #TODO confirm all files with unique names???
    #TODO confirm readable cloud dir ??
    #TODO do we care if file already exists?

	for key, value in od.items(): 
		bucket = get_bucket_name(value)
		s3_file = basename(key)
		print('attempting to upload ' + s3_file + ' to s3://' + bucket)
   
	   # Upload the file
		try:
			computed_checksum = calculate_s3_etag(key)
			aws_client.upload_file(key, bucket, s3_file)
			response = aws_client.head_object(Bucket=bucket, Key=s3_file)
			file_size = response['ContentLength']
			print ("size:", file_size)
			s3_path = 's3://' + bucket + '/' + s3_file
			md5sum = response['ETag'][1:-1]
			print('checksum check:', computed_checksum, ':', md5sum)
			if (computed_checksum == md5sum):
				print('same checksum')
			else:
				print('different checksum') 
			value['s3_path'] = s3_path
			value['file_size'] = file_size
			value['md5sum'] = md5sum
		except ClientError as e:
			logging.error(e)
			print(e)
			value['s3_path'] = ''
			value['file_size'] = -1
			value['md5sum'] = ''

# code taken from https://stackoverflow.com/questions/12186993/what-is-the-algorithm-to-compute-the-amazon-s3-etag-for-a-file-larger-than-5gb#answer-19896823
def calculate_s3_etag(file_path, chunk_size=8 * 1024 * 1024):
    md5s = []

    with open(file_path, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))

    if len(md5s) < 1:
        return '{}'.format(hashlib.md5().hexdigest())

    if len(md5s) == 1:
        return '{}'.format(md5s[0].hexdigest())

    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '{}-{}'.format(digests_md5.hexdigest(), len(md5s))
    			
def upload_to_gcloud(od):    
	for key, value in od.items(): 
		bucket = get_bucket_name(value)
		file = basename(key)
		print('attempting to upload ' + file + ' to gs://' + bucket)   
		 # FIXME upload file

def get_bucket_name(row):
	return row['study_id'] + '-' + row['consent_code']
	
def create_manifest_file(manifest_filepath, od):
	isfirstrow = True
	with open(manifest_filepath, 'wt') as out_file:
		tsv_writer = csv.writer(out_file, delimiter='\t')
		for key, value in od.items():
			if (isfirstrow):
				# print header row
				tsv_writer.writerow(value.keys())
				isfirstrow = False 
			tsv_writer.writerow(value.values())
		out_file.close()
	print(od)
       
if __name__ == '__main__':
    main()
