#!/usr/bin/env python3

# This script was written for the NIH BioData Catalyst to process an input manifest file
# containing file locations and optional file metadata, and upload the files to Amazon and Google 
# Cloud services.
#
# usage: process_localdir_to_aws.py [-h] --tsv TSV --bucket BUCKET 
# [--bucket_folder BUCKET_FOLDER] [--test] [--resume] [--checksum_threads CHECKSUM_THREADS] 
# [--upload_threads UPLOAD_THREADS] [--chunk-size CHUNK_SIZE]
#
# required arguments:
#
# --tsv					local file path to input manifest file 
# --bucket				aws bucket name
#
# optional arguments:
# -h, --help            show help message
# --test                test mode: confirm input manifest file is valid
# --resume              run process in RESUME mode, with the given manifest file
# --checksum_threads CHECKSUM_THREADS     number of concurrent threads for calculating checksums (default: number of CPUs on machine)
# --upload_threads UPLOAD_THREADS	number of concurrent threads for uploading (default: 1)
# --chunk-size CHUNK_SIZE
#                       mulipart-chunk-size for uploading (default: 64 * 1024 * 1024)

# FIXME sort files by size, then thread cases 

import argparse
import subprocess
import datetime
import hashlib
import fileinput
import csv
import signal
import sys
import time
import io
import os
import tempfile
import uuid
from os import access, R_OK
from os.path import isfile, basename
from collections import OrderedDict 
from urllib.parse import urlparse

import threading
import concurrent.futures
import multiprocessing.pool

# for aws s3
import logging
import boto3
import botocore

# for google storage
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.api_core.exceptions import BadRequest, Forbidden
from google.cloud.exceptions import NotFound
import base64
import struct
import crcmod

out_file_path = ''
out_file = ''
bucket_name = ''
bucket_folder = ''
all_files_readable = True
boto_multipart_threshold = 64 * 1024 * 1024
regular_uploads = []
multipart_uploads = []

# 1. Read and Verify Input Manifest File
# 2. If --gs is set, then perform google checksums and upload to Google Cloud
# 3. If -aws is set, then perform aws checksums and upload to AWS
# 4. Write out receipt manifest file

def main():
	args = parse_args()
	print('Script running version 1.4 on', sys.platform, 'with', os.cpu_count(), 'cpus')

	# process file
	od = OrderedDict()
	read_and_verify_file(od, args) 

	global out_file
	out_file = get_receipt_manifest_file_pointer(args.tsv.name)	

	calculate_aws_checksums(od, args.checksum_threads, args.chunk_size, out_file, args.resume)
	categorize_files_for_upload(od, args.checksum_threads, out_file, args.resume)
	upload_to_aws(od, out_file, args.upload_threads, args.chunk_size, args.resume)

	# do one last refresh in case the resume file was actually complete
	if (args.resume):
		update_manifest_file(out_file, od)
				
	out_file.close()
	upload_manifest_file(out_file)
	print("Done. Receipt manifest located at", out_file_path)

# Generate name for receipt manifest file by replacing ".tsv" in input manifest file with
# ".<datetime>.manifest.tsv" and return file pointer to it.

def get_receipt_manifest_file_pointer(input_manifest_file_path):
	manifest_filepath = input_manifest_file_path
	timestr = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
	
	if (manifest_filepath.endswith('.tsv')):
		manifest_filepath = manifest_filepath.replace(".tsv", ".manifest." + timestr + ".tsv")	
	else:
		manifest_filepath += '.manifest.' + timestr + '.tsv'
	global out_file_path
	out_file_path = manifest_filepath
	f = open(manifest_filepath, 'wt')
	return f		

# upload receipt manifest file to each cloud bucket that had content uploaded

def upload_manifest_file(receipt_manifest_file):
	aws_client = boto3.client('s3')
	print("Uploading ", receipt_manifest_file.name, " to s3://", bucket_name, bucket_folder, sep='')
	aws_client.upload_file(receipt_manifest_file.name, bucket_name + bucket_folder, basename(receipt_manifest_file.name))

# Parse user arguments and confirm valid arguments.

def parse_args():
	parser = argparse.ArgumentParser(description='Process TSV file.')
	parser.add_argument('--tsv', required=True, type=argparse.FileType('r'), help='tsv file')
	parser.add_argument('--bucket', required=True, help='AWS bucket name')
	parser.add_argument('--bucket_folder', required=False, help='AWS bucket folder name')
	parser.add_argument('--test', default=False, action='store_true', help='test mode: confirm input manifest file is valid')
	parser.add_argument('--resume', default=False, action='store_true', help='run process in RESUME mode, with the given manifest file')
	parser.add_argument('--checksum_threads', type=int, default=os.cpu_count(), help='number of concurrent checksum threads (default: number of CPUs on machine)')
	parser.add_argument('--upload_threads', type=int, default=1, help='number of concurrent upload threads (default: 1)')
	parser.add_argument('--chunk_size', type=int, default=8 * 1024 * 1024, help='mulipart-chunk-size for uploading (default: 8 * 1024 * 1024)')

	global bucket_name
	global bucket_folder
		
	args = parser.parse_args()
	if (len(sys.argv) == 0):
		parser.print_help()
	bucket_name = args.bucket 
	if (args.bucket_folder):
		bucket_folder = '/' + args.bucket_folder
	return args

# For each row in the input manifest file, confirm that file is readable, and buckets to
# write to are writeable by the user.

def read_and_verify_file(od, args) :
	reader = csv.DictReader(args.tsv, dialect='excel-tab')
	all_files_readable = True

	if(verify_aws_bucket(od, args.test) == False):
		print("Script exiting due to errorrs with bucket permissions.")
		exit()

	start = datetime.datetime.now()
	for row in reader:
		input_file = row['input_file_path']
		od[input_file] = row
		process_row(row, args.test, args.resume)
	end = datetime.datetime.now()
	print('Elapsed time for read_and_verify_file:', end - start)
	
	if (all_files_readable == False):
		print("Script exiting due to errorrs in ", args.tsv.name)
		exit()
	if (args.test):
		print("Test mode:", args.tsv.name, "valid. Script now exiting.")
		exit()

# For each row, add the information to the ordered dictionary for the manifest file. 
					
def process_row(row, test_mode, resume_mode):
	input_file = row['input_file_path']

	# Add blank fields to ensure that they will appear in the correct order for the
	# receipt manifest file
	if (not resume_mode):
		row['file_name'] = basename(input_file)
		row['file_size'] = ''
		row['guid'] = ''
		row['ga4gh_drs_uri'] = ''
		row['md5sum'] = ''
		row['s3_md5sum'] =''
		row['s3_path'] = ''
		row['s3_modified_date'] = ''
		row['s3_file_size'] = ''


	if (isfile(input_file) and access(input_file, R_OK)):
		row['file_size'] = os.path.getsize(input_file)
		if (test_mode):
			print("File is readable:", input_file)				
	else:
		print("File doesn't exist or isn't readable:", input_file)
		global all_files_readable
		all_files_readable = False

# Confirm all AWS writeable by the user		
def verify_aws_bucket(od, test_mode):
 	iam = boto3.client('iam')
 	sts = boto3.client('sts')
 	arn = sts.get_caller_identity()['Arn']
 
 	return aws_bucket_writeable(bucket_name, iam, arn, test_mode)

# Spawn threads to calculate AWS checksums

def calculate_aws_checksums(od, num_threads, chunk_size, out_file, resume_mode):
	print('Calculating aws checksums with', num_threads, 'threads')
	start = datetime.datetime.now()
	with concurrent.futures.ThreadPoolExecutor(num_threads) as executor:	
		futures = [executor.submit(calculate_s3_md5sum, value['input_file_path'], value, chunk_size, od, out_file, resume_mode) for key, value in od.items()]
		for idx, future in enumerate(concurrent.futures.as_completed(futures)):
			try:
				res = future.result()
			except ValueError as e:
				print(e)
	end = datetime.datetime.now()
	print('Elapsed time for calculate_aws_checksums:', end - start)

# Categorize files into Already Uploaded, Large, and Small

def categorize_files_for_upload(od, num_threads, out_file, resume_mode):
	print('Categorizing files for upload with', num_threads, 'threads')
	start = datetime.datetime.now()
	with concurrent.futures.ThreadPoolExecutor(num_threads) as executor:	
		futures = [executor.submit(categorize_file_for_upload, value, resume_mode) for key, value in od.items()]
		for idx, future in enumerate(concurrent.futures.as_completed(futures)):
			try:
				res = future.result()
			except ValueError as e:
				print(e)
	end = datetime.datetime.now()
	print('Elapsed time for categorize_files_for_upload:', end - start)
	update_manifest_file(out_file, od)

def categorize_file_for_upload(value, resume_mode):
	input_file_path = value['input_file_path']

	if (resume_mode and 's3_path' in value.keys() and value['s3_path'].startswith('s3://')):
# fixme get drs_uri value
		add_new_drs_uri(value)
#			add_drs_uri_from_path(value, value['s3_path'])
		print("Already uploaded. Skipping", value['s3_path'])
		return

	s3_file = bucket_folder + value['input_file_path']
	if (path_in_aws_bucket(bucket_name, s3_file, value)):
		print("Already exists. Skipping", value['s3_path'])
		return

	global regular_uploads
	global multipart_uploads
		
	if (value['file_size'] < boto_multipart_threshold):
		print("appended", value['input_file_path'], "to regular_uploads")
		regular_uploads.append(value)
	else:
		multipart_uploads.append(value)
		print("appended", value['input_file_path'], "to multipart_uploads")

# Determines upload type based in input_file_path and uploads to AWS
 
def upload_to_aws(od, out_file, threads, chunk_size, resume_mode): 
	aws_client = boto3.client('s3')
	transfer_config_regular = boto3.s3.transfer.TransferConfig(multipart_threshold=boto_multipart_threshold, multipart_chunksize=chunk_size, use_threads=False)  
	transfer_config_multipart = boto3.s3.transfer.TransferConfig(multipart_threshold=boto_multipart_threshold, multipart_chunksize=chunk_size, max_concurrency=threads, use_threads=True)  

#	print('Attempting to upload to s3://', bucket_name, '/', bucket_folder, ' with threads=', threads, ' and chunk_size=', chunk_size, sep='')

	global regular_uploads
	global multipart_uploads

	start = datetime.datetime.now()
	with concurrent.futures.ThreadPoolExecutor(threads) as executor:	
		futures = [executor.submit(handle_aws_file_upload, aws_client, transfer_config_regular, value['input_file_path'], bucket_name, bucket_folder + value['input_file_path'], value) for value in regular_uploads]
		for idx, future in enumerate(concurrent.futures.as_completed(futures)):
			try:
				res = future.result()
			except ValueError as e:
				print(e)
	end = datetime.datetime.now()
	print('\nElapsed time for multipart aws uploads:', end - start)
	update_manifest_file(out_file, od)	


	start = datetime.datetime.now()
	with concurrent.futures.ThreadPoolExecutor(1) as executor:	
		futures = [executor.submit(handle_aws_file_upload, aws_client, transfer_config_multipart, value['input_file_path'], bucket_name, bucket_folder + value['input_file_path'], value) for value in multipart_uploads]
		for idx, future in enumerate(concurrent.futures.as_completed(futures)):
			try:
				res = future.result()
			except ValueError as e:
				print(e)
	end = datetime.datetime.now()
	print('\nElapsed time for multipart aws uploads:', end - start)
	update_manifest_file(out_file, od)	

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100

            sys.stdout.write(
                "\rupload %s: %s / %s (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

# Upload local file to AWS
def handle_aws_file_upload(aws_client, transfer_config, from_file, bucket_name, s3_file, value):
	if (1 == 2 and value['s3_path'].startswith('s3://')):
		print("Already exists. Skipping", value['s3_path'])		
		return

#	print('Attempting to upload ', from_file, ' to s3://', bucket_name, '/', s3_file, sep='')
#	start = datetime.datetime.now()
	aws_client.upload_file(from_file, bucket_name, s3_file, Config=transfer_config, Callback=ProgressPercentage(from_file))
	end = datetime.datetime.now()
#	print('Elapsed time for aws upload:', end - start)
	response = aws_client.head_object(Bucket=bucket_name, Key=s3_file)
	if (checksum_check(value, response)):
		add_aws_manifest_metadata(value, response, 's3://' + bucket_name + '/' + s3_file)	

# Confirm AWS bucket writeable by the user

def aws_bucket_writeable(bucket_name, iam, arn, test_mode):
	# Create an arn representing the objects in a bucket
	bucket_objects_arn = 'arn:aws:s3:::%s/*' % bucket_name

	try:
		s3 = boto3.resource('s3')
		if (not(s3.Bucket(bucket_name) in s3.buckets.all())):
			print('ERROR: s3 bucket does not exist -', bucket_name)
			return False
			
		# Run the policy simulation for the PUT
		results = iam.simulate_principal_policy(
			PolicySourceArn=arn,
			ResourceArns=[bucket_objects_arn],
			ActionNames=['s3:PutObject']
		)
#			print(results)
		if (results['EvaluationResults'][0]['EvalDecision'] == 'allowed'):
			if (test_mode):
				print("Bucket is writeable: s3://", bucket_name, sep='')			
			return True
		else:
			print('ERROR: s3 bucket does not exist or is not writeable -', bucket_name)
			return False
	except botocore.errorfactory.NoSuchBucket as e:
		print('ERROR: s3 bucket does not exist or is not writeable -', bucket_name)
		return False		

def path_in_aws_bucket(bucket_name, path, value):
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(bucket_name)
	objs = list(bucket.objects.filter(Prefix=path))
	if (len(objs) > 0):
		aws_client = boto3.client('s3')
		response = aws_client.head_object(Bucket=bucket_name, Key=objs[0].key)
		if (checksum_check(value, response)):
			add_aws_manifest_metadata(value, response, 's3://' + bucket_name + '/' + objs[0].key)	
			return True
	return False

def aws_key_exists(bucket_name, key):
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(bucket_name)
	objs = list(bucket.objects.filter(Prefix=key))
	if len(objs) > 0 and objs[0].key == key:
		return True
	else:
		return False

def download_aws_key(bucket_name, key, download_path_name):
	s3 = boto3.client('s3')
	s3.download_file(bucket_name, key, download_path_name)
	print('downloaded s3://%s/%s to %s' % (bucket_name, key, download_path_name))				

def checksum_check(fields, response):
	md5sum = response['ETag'][1:-1]
#	print('checksum check:', fields['s3_md5sum'], ':', md5sum)
	if (fields['s3_md5sum'] == md5sum):
#		print("matches")
		return True
	else:
		print("checksum mismatch", fields['s3_md5sum'], ':', md5sum)
		return False

# Given the response returned from s3, get metadata for the object and add to the 
# ordered dictionary

def add_aws_manifest_metadata(fields, response, path):
	file_size = response['ContentLength']
#	print ("size:", file_size)
	md5sum = response['ETag'][1:-1]
	if ("-" not in md5sum):
		fields['md5sum'] = md5sum
	fields['s3_path'] = path
	fields['s3_modified_date'] = format(response['LastModified'])
	fields['s3_file_size'] = file_size
	if (len(fields['md5sum']) == 0):
		md5sum = calculate_md5sum(fields['input_file_path'])
		fields['md5sum'] = md5sum
	if (not fields['ga4gh_drs_uri'].startswith("drs://")):
		add_new_drs_uri(fields)
# fixme - figure out existing uuid?
#		add_drs_uri_from_path(fields, path)

def add_blank_aws_manifest_metadata(od):
	for key, value in od.items():
		if ('s3_md5sum' not in value): 
			value['s3_md5sum'] =''
		value['s3_path'] = ''
		value['s3_modified_date'] = ''
		value['s3_file_size'] = ''

# AWS typically performs multipart uploads for larger files, and rather than compute an
# md5sum value on the entire file, it calculates a hash value over the parts of the file.
# This algorithm is simulated here, in order to confirm that the checksum returned from
# AWS matches.
#
# code adapted from https://stackoverflow.com/questions/12186993/what-is-the-algorithm-to-compute-the-amazon-s3-etag-for-a-file-larger-than-5gb#answer-19896823
# more discussion of how checksum is calculated for s3 here: https://stackoverflow.com/questions/6591047/etag-definition-changed-in-amazon-s3/28877788#28877788
def calculate_s3_md5sum(input_file_path, value, chunk_size, od, out_file, resume_mode):
	if (input_file_path.startswith('gs://') or input_file_path.startswith('s3://')):
		#don't calculate checksum here
		return

	if (resume_mode and 's3_md5sum' in value.keys() and len(value['s3_md5sum']) > 0):
		print('s3_md5sum already calculated for', input_file_path)
		return

	if (value['file_size'] < boto_multipart_threshold):
		md5sum = calculate_md5sum(input_file_path)
		value['s3_md5sum'] = md5sum
		value['md5sum'] = md5sum		
	else:		
		md5s = []
		start = datetime.datetime.now()
		with open(input_file_path, 'rb') as fp:
			while True:
				data = fp.read(chunk_size)
				if not data:
					break
				md5s.append(hashlib.md5(data))

		computed_checksum = ''
		if len(md5s) < 1:
			computed_checksum = '{}'.format(hashlib.md5().hexdigest())
		elif len(md5s) == 1:
			computed_checksum = '{}'.format(md5s[0].hexdigest())
		else:
			digests = b''.join(m.digest() for m in md5s)
			digests_md5 = hashlib.md5(digests)
			computed_checksum = '{}-{}'.format(digests_md5.hexdigest(), len(md5s))

		end = datetime.datetime.now()
		print('elapsed time for checksum', computed_checksum, ':', end - start)
		value['s3_md5sum'] = computed_checksum

	update_manifest_file(out_file, od)

#https://stackoverflow.com/questions/3910071/check-file-size-on-s3-without-downloading/12678543
#https://docs.aws.amazon.com/cli/latest/reference/s3api/head-object.html
def get_s3_file_size(s3_path):
	obj = urlparse(s3_path, allow_fragments=False)
	aws_client = boto3.client('s3')
	response = aws_client.head_object(Bucket=obj.netloc, Key=obj.path.lstrip('/'))
	return response['ContentLength']


def update_md5sum(md5, buffer):
	start = datetime.datetime.now()			
	md5.update(buffer)
	end = datetime.datetime.now()	
#	print('elapsed time for update_md5sum:', end - start)


def update_s3_md5sum(md5s, buffer):
	start = datetime.datetime.now()			
	md5s.append(hashlib.md5(buffer))
	end = datetime.datetime.now()	
#	print('elapsed time for update_md5sum:', end - start)

# Calculate md5sum for the path, including downloading the file if it is a cloud resource.

def calculate_md5sum(input_file_path):
	local_file = input_file_path
	
	print("Calculating md5sum for", input_file_path)
	
	if (isfile(local_file) and access(local_file, R_OK)):
		m = hashlib.md5()
		with open(local_file, 'rb') as fp:
			while True:
				data = fp.read(66 * 1024 * 1024)
				if not data:
					break
				m.update(data)
		return m.hexdigest()
	else:
		print("Not a valid path in calculate_md5sum: ", input_file_path)	
		return
		
def get_drs_uri():
	x = uuid.uuid4()
	return "drs://dg.4503:dg.4503/" + str(x)

def add_drs_uri_from_path(value, path):
	if (not value['ga4gh_drs_uri'].startswith('drs://')):
		segments = path.split('/')
#		print("path:", path, "segments:", segments)
		value['guid'] = 'dg.4503/' + segments[4]
		value['ga4gh_drs_uri'] = "drs://dg.4503:dg.4503/" + segments[4]

def add_new_drs_uri(value):
	if (not value['ga4gh_drs_uri'].startswith('drs://')):
		x = uuid.uuid4()		
		value['guid'] = 'dg.4503/' + str(x)
		value['ga4gh_drs_uri'] = "drs://dg.4503:dg.4503/" + str(x)
	
# This method gets called after each checksum and upload so that as much state as possible
# is written out to the receipt manifest file.

def update_manifest_file(f, od):
	with threading.Lock():
		# start from beginning of file
		f.seek(0)
		f.truncate()
	
		isfirstrow = True
		tsv_writer = csv.writer(f, delimiter='\t')
		for key, value in od.items():
			if (isfirstrow):
				# print header row
				tsv_writer.writerow(value.keys())
				isfirstrow = False 
			tsv_writer.writerow(value.values())
	# we don't close the file until the end of the operation
	
# If a quit is detected, then this method is called to save state.
#
# adapted from here: https://stackoverflow.com/questions/18114560/python-catch-ctrl-c-command-prompt-really-want-to-quit-y-n-resume-executi/18115530

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
