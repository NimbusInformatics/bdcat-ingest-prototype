#!/usr/bin/env python3

# This script was written for the NIH BioData Catalyst to process an input manifest file
# containing file locations and file metadata, and upload the files to Amazon and Google 
# Cloud services.
#
# usage: process.py [-h] --tsv TSV [--gs] [--aws] [--test] [--resume]
#                  [--threads THREADS] [--chunk-size CHUNK_SIZE]
#
# required arguments:
#
# --tsv					local file path to input manifest file 
#
# --gs                  upload to Google Cloud
# --aws                 upload to AWS
# Either --gs or --aws needs to be specified. Both arguments can also be specified. 
#
# optional arguments:
# -h, --help            show help message
# --test                test mode: confirm input manifest file is valid
# --resume              run process in RESUME mode, with the given manifest file
# --threads THREADS     number of concurrent threads (default: number of CPUs on machine)
# --chunk-size CHUNK_SIZE
#                        mulipart-chunk-size for uploading (default: 8 * 1024 * 1024)


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
gs_crc32c = {}
gs_buckets = {}
aws_buckets = {}

# 1. Read and Verify Input Manifest File
# 2. If --gs is set, then perform google checksums and upload to Google Cloud
# 3. If -aws is set, then perform aws checksums and upload to AWS
# 4. Write out receipt manifest file

def main():
	args = parse_args()
	print('Script running on', sys.platform, 'with', os.cpu_count(), 'cpus')

	# process file
	od = OrderedDict()
	read_and_verify_file(od, args) 

	global out_file
	out_file = get_receipt_manifest_file_pointer(args.tsv.name)	

	if (args.gs):
		calculate_gs_checksums(od, args.threads, out_file, args.resume)
		upload_to_gcloud(od, out_file, args.threads, args.chunk_size, args.resume)

	if (args.aws):
		calculate_aws_checksums(od, args.threads, args.chunk_size, out_file, args.resume)
		upload_to_aws(od, out_file, args.threads, args.chunk_size, args.resume)

	# do one last refresh in case the resume file was actually complete
	if (args.resume):
		update_manifest_file(out_file, od)
				
	out_file.close()
	upload_manifest_file(out_file, args.gs, args.aws)
	print("Done. Receipt manifest located at", out_file_path)

# Generate name for receipt manifest file by replacing ".tsv" in input manifest file with
# ".<datetime>.manifest.tsv" and return file pointer to it.

def get_receipt_manifest_file_pointer(input_manifest_file_path):
	manifest_filepath = input_manifest_file_path
	timestr = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
	
	if (manifest_filepath.endswith('.tsv')):
		manifest_filepath = manifest_filepath.replace(".tsv", "." + timestr + ".manifest.tsv")	
	else:
		manifest_filepath += '.manifest.tsv'
	global out_file_path
	out_file_path = manifest_filepath
	f = open(manifest_filepath, 'wt')
	return f		

# upload receipt manifest file to each cloud bucket that had content uploaded

def upload_manifest_file(receipt_manifest_file, gs_upload, aws_upload):
	if (gs_upload):
		storage_client = storage.Client()
		for key, value in gs_buckets.items():
			print("Uploading ", receipt_manifest_file.name, " to gs://", key, sep='')
			bucket = storage_client.bucket(key)
			blob = bucket.blob(basename(receipt_manifest_file.name))
			blob.upload_from_filename(receipt_manifest_file.name)

				
	if (aws_upload):
		aws_client = boto3.client('s3')
		for key, value in aws_buckets.items():
			print("Uploading ", receipt_manifest_file.name, " to s3://", key, sep='')
			aws_client.upload_file(receipt_manifest_file.name, key, basename(receipt_manifest_file.name))
	


# Parse user arguments and confirm valid arguments.

def parse_args():
	parser = argparse.ArgumentParser(description='Process TSV file.')
	parser.add_argument('--tsv', required=True, type=argparse.FileType('r'), help='tsv file')
	parser.add_argument('--gs', default=False, action='store_true', help='upload to Google Cloud')
	parser.add_argument('--aws', default=False, action='store_true', help='upload to AWS')
	parser.add_argument('--test', default=False, action='store_true', help='test mode: confirm input manifest file is valid')
	parser.add_argument('--resume', default=False, action='store_true', help='run process in RESUME mode, with the given manifest file')
	parser.add_argument('--threads', type=int, default=os.cpu_count(), help='number of concurrent threads (default: number of CPUs on machine)')
	parser.add_argument('--chunk-size', type=int, default=8 * 1024 * 1024, help='mulipart-chunk-size for uploading (default: 8 * 1024 * 1024)')
	
	args = parser.parse_args()
	if (len(sys.argv) == 0):
		parser.print_help()
	if (not args.gs and not args.aws):
		print('Error: Either gs or aws needs to be set')
		parser.print_help()
		exit()
	return args

# For each row in the input manifest file, confirm that file is readable, and buckets to
# write to are writeable by the user.

def read_and_verify_file(od, args) :
	reader = csv.DictReader(args.tsv, dialect='excel-tab')
	all_files_readable = True
	all_buckets_writeable = True
	for row in reader:
		if(process_row(od, row, args.test, args.resume) == False):
			all_files_readable = False
	if (args.gs):
		if(verify_gs_buckets(od, args.test) == False):
			all_buckets_writeable = False
	if (args.aws):
		if(verify_aws_buckets(od, args.test) == False):
			all_buckets_writeable = False
	if (all_files_readable == False or all_buckets_writeable == False):
		print("Script exiting due to errorrs in ", args.tsv.name)
		exit()
	if (args.test):
		print("Test mode:", args.tsv.name, "valid. Script now exiting.")
		exit()

# For each row, add the information to the ordered dictionary for the manifest file. 
					
def process_row(od, row, test_mode, resume_mode):
	study_id = row['study_id']
	local_file = row['input_file_path']
	# Add blank fields to ensure that they will appear in the correct order for the
	# receipt manifest file
	if (not resume_mode):
		row['md5sum'] = ''
		row['gs_crc32c'] = ''
		row['gs_path'] = ''
		row['gs_modified_date'] = ''
		row['gs_file_size'] = ''
		row['s3_md5sum'] =''
		row['s3_path'] = ''
		row['s3_modified_date'] = ''
		row['s3_file_size'] = ''

	od[study_id + '_' + local_file] = row
	# confirm file exists and is readable
	if(local_file.startswith("gs://")):
		return verify_gs_file(row, local_file, test_mode)
	elif(local_file.startswith("s3://")):
		return verify_s3_file(row, local_file, test_mode)
	elif (isfile(local_file) and access(local_file, R_OK)):
		if (test_mode):
			print("File is readable:", local_file)			
		return True		
	else:
		print("File doesn't exist or isn't readable:", local_file)
		return False

# Confirm file on Google Cloud, and include add its checksum information
def verify_gs_file(value, local_file, test_mode):
	obj = urlparse(local_file, allow_fragments=False)
	
	storage_client = storage.Client()
	bucket = storage_client.bucket(obj.netloc)
	blob = bucket.blob(obj.path.lstrip('/'))
	if (blob.exists()):
		blob.reload()
		value['gs_crc32c'] = blob.crc32c
#		print('in gs_blob_exists, gs_crc32c=', value['gs_crc32c'])
		global gs_crc32c
		unsigned_int = format(struct.unpack('>I', base64.b64decode(value['gs_crc32c']))[0])
		gs_crc32c[value['input_file_path']] = unsigned_int	
		if (blob.md5_hash):
			value['md5sum'] = blob.md5_hash
		if (test_mode):
			print('Location exists:', local_file)
		return True    
	else:
		print('Not found:', local_file)
		return False

# Confirm file on AWS, and include add its checksum information
       
def verify_s3_file(value, local_file, test_mode):
	obj = urlparse(local_file, allow_fragments=False)
	if (aws_key_exists(obj.netloc, obj.path.lstrip('/'))):
		value['s3_md5sum'] = boto3.client('s3').head_object(Bucket=obj.netloc, Key=obj.path.lstrip('/'))['ETag'][1:-1]
		if ('-' not in value['s3_md5sum']):
			value['md5sum'] = value['s3_md5sum']
		if (test_mode):
			print('Location exists:', local_file)
		return True    
	else:
		print('Not found:', local_file)
		return False

# Confirm all Google Storage buckets writeable by the user
    	   
def verify_gs_buckets(od, test_mode):
	global gs_buckets
	storage_client = storage.Client()

	all_buckets_writeable = True

	for key, value in od.items(): 
		bucket_name = get_bucket_name(value) 
		if (gs_bucket_writeable(bucket_name, storage_client, gs_buckets, test_mode) == False):		
			all_buckets_writeable = False
	return all_buckets_writeable

# Confirm all AWS writeable by the user
		
def verify_aws_buckets(od, test_mode):
	global aws_buckets
	iam = boto3.client('iam')
	sts = boto3.client('sts')
	arn = sts.get_caller_identity()['Arn']
	all_buckets_writeable = True

	for key, value in od.items(): 
		bucket_name = get_bucket_name(value)
		if (aws_bucket_writeable(bucket_name, iam, arn, aws_buckets, test_mode) == False):
			all_buckets_writeable = False
	return all_buckets_writeable

# Spawn threads to calculate AWS checksums

def calculate_aws_checksums(od, num_threads, chunk_size, out_file, resume_mode):
	print('Calculating aws checksums with', num_threads, 'threads')
	start = datetime.datetime.now()
	with concurrent.futures.ThreadPoolExecutor(num_threads) as executor:	
		futures = [executor.submit(calculate_s3_md5sum, value['input_file_path'], value, chunk_size, od, out_file, resume_mode) for key, value in od.items()]
#		print("Executing total", len(futures), "jobs")

		for idx, future in enumerate(concurrent.futures.as_completed(futures)):
			try:
				res = future.result()
			except ValueError as e:
				print(e)
	end = datetime.datetime.now()
#	print('elapsed time for aws checksums:', end - start)

# Determines upload type based in input_file_path and uploads to AWS
 
def upload_to_aws(od, out_file, threads, chunk_size, resume_mode): 
	aws_client = boto3.client('s3')
	transfer_config = boto3.s3.transfer.TransferConfig(multipart_chunksize=chunk_size, max_concurrency=threads, use_threads=True)  


	tmpfilepointer = ''
	tmpfilepath = ''
	for key, value in od.items():
		if (resume_mode and 's3_path' in value.keys() and value['s3_path'].startswith('s3://')):
			print("Already uploaded. Skipping", value['s3_path'])
			continue 
		try: 
			if (value['input_file_path'].startswith('gs://')):
				# download the file to a temporary location
				(tmpfilepointer, tmpfilepath) = tempfile.mkstemp()
				obj = urlparse(value['input_file_path'], allow_fragments=False)			
				download_gs_key(obj.netloc, obj.path.lstrip('/'), tmpfilepath)
				calculate_s3_md5sum(tmpfilepath, value, chunk_size, od, out_file, resume_mode)

			bucket_name = get_bucket_name(value)
			s3_file = value['s3_md5sum'] + '/' + basename(value['input_file_path'])
			print('Attempting to upload ', s3_file, ' to s3://', bucket_name, ' with threads=', threads, ' and chunk_size=', chunk_size, sep='')
		
			if (aws_key_exists(bucket_name, s3_file)):
				print("Already exists. Skipping ", 's3://', bucket_name, '/', s3_file, sep='')
				response = aws_client.head_object(Bucket=bucket_name, Key=s3_file)
				add_aws_manifest_metadata(value, response, 's3://' + bucket_name + '/' + s3_file)
			else:
				if (value['input_file_path'].startswith("s3://")):
					handle_aws_copy(value, bucket_name, s3_file)
				elif (value['input_file_path'].startswith("gs://")):
					handle_aws_file_upload(aws_client, transfer_config, tmpfilepath, bucket_name, s3_file)
				else:	
					handle_aws_file_upload(aws_client, transfer_config, value['input_file_path'], bucket_name, s3_file)
				response = aws_client.head_object(Bucket=bucket_name, Key=s3_file)
				add_aws_manifest_metadata(value, response, 's3://' + bucket_name + '/' + s3_file)
		except botocore.exceptions.ClientError as e:
			logging.error(e)
			print(e)
		finally:
			if (tmpfilepath):
				# remove temp file
				os.remove(tmpfilepath)
				tmpfilepath = ''							

		update_manifest_file(out_file, od)	

# Upload local file to AWS

def handle_aws_file_upload(aws_client, transfer_config, from_file, bucket_name, s3_file):
	start = datetime.datetime.now()
	aws_client.upload_file(from_file, bucket_name, s3_file, Config=transfer_config)
	end = datetime.datetime.now()
	print('elapsed time for aws upload:', end - start)


# Copy s3 -> s3
# See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy

def handle_aws_copy(value, tobucket, tokey):
	obj = urlparse(value['input_file_path'], allow_fragments=False)
		
	s3 = boto3.resource('s3')
	copy_source = {
		'Bucket': obj.netloc,
		'Key': obj.path.lstrip('/')
	}
	s3.meta.client.copy(copy_source, tobucket, tokey)

# Confirm AWS bucket writeable by the user

def aws_bucket_writeable(bucket_name, iam, arn, aws_buckets, test_mode):
	if (bucket_name in aws_buckets):
		if (aws_buckets[bucket_name] == 1):
			return True
		else:
			return False
	else:
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
				aws_buckets[bucket_name] = 1
				if (test_mode):
					print("Bucket is writeable: s3://", bucket_name, sep='')			
				return True
			else:
				aws_buckets[bucket_name] = 0
				print('ERROR: s3 bucket does not exist or is not writeable -', bucket_name)
				return False
		except botocore.errorfactory.NoSuchBucket as e:
			aws_buckets[bucket_name] = 0
			print('ERROR: s3 bucket does not exist or is not writeable -', bucket_name)
			return False
		
# Confirm Google Storage bucket writeable by the user

def gs_bucket_writeable(bucket_name, storage_client, gs_buckets, test_mode):
	if (bucket_name in gs_buckets):
		if (gs_buckets[bucket_name] == 1):
			return True
		else:
			return False
	else:
		try:
			bucket = storage_client.get_bucket(bucket_name)
			if (bucket.exists()):
				returnedPermissions = bucket.test_iam_permissions('storage.objects.create')
#				print('PERMISSIONS for', bucket_name, returnedPermissions)
				if ('storage.objects.create' in returnedPermissions):
					gs_buckets[bucket_name] = 1
					return True
				else:
					print('ERROR: gs bucket is not writeable', bucket_name)
					gs_buckets[bucket_name] = 0
					return False					
		except BadRequest as e:
			gs_buckets[bucket_name] = 0
			print('ERROR: gs bucket does not exist -', bucket_name, e)
			return False
		except Forbidden as e2:
			gs_buckets[bucket_name] = 0
			print('ERROR: gs bucket is not accessible by user -', bucket_name, e2)
			return False
		except Exception as e3:
			print(e3)
			print('ERROR: gs bucket does not exist or is not accessible by user -', bucket_name, e3)
			gs_buckets[bucket_name] = 0
			return False			

def aws_key_exists(bucket_name, key):
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(bucket_name)
	objs = list(bucket.objects.filter(Prefix=key))
	if len(objs) > 0 and objs[0].key == key:
		return True
	else:
		return False

def gs_blob_exists(value, bucket_name, key):
	storage_client = storage.Client()
	bucket = storage_client.bucket(bucket_name)
	blob = bucket.blob(key)
	if (blob.exists()):
		blob.reload()
		value['gs_crc32c'] = blob.crc32c
		print('in gs_blob_exists, gs_crc32c=', value['gs_crc32c'])
		global gs_crc32c
		unsigned_int = format(struct.unpack('>I', base64.b64decode(value['gs_crc32c']))[0])
		gs_crc32c[value['input_file_path']] = unsigned_int

# Given a gs:// path, download it to download_path_name

def download_gs_key(bucket_name, key, download_path_name):
	subprocess.check_call([
		'gsutil',
		'-o', 'GSUtil:parallel_composite_upload_threshold=%s' % ('150M'),
		'cp', 'gs://%s/%s' % (bucket_name, key), download_path_name
	])
	print('downloaded gs://%s/%s to %s' % (bucket_name, key, download_path_name))				

# Given an s3:// path, download it to download_path_name

def download_aws_key(bucket_name, key, download_path_name):
	s3 = boto3.client('s3')
	s3.download_file(bucket_name, key, download_path_name)
	print('downloaded s3://%s/%s to %s' % (bucket_name, key, download_path_name))				

# Given the response returned from s3, get metadata for the object and add to the 
# ordered dictionary

def add_aws_manifest_metadata(fields, response, path):
	file_size = response['ContentLength']
	print ("size:", file_size)
	md5sum = response['ETag'][1:-1]
	if ("-" not in md5sum):
		fields['md5sum'] = md5sum
	print('checksum check:', fields['s3_md5sum'], ':', md5sum)
	if (fields['s3_md5sum'] == md5sum):
		print('same checksum')
	else:
		print('different checksum')
	fields['s3_path'] = path
	fields['s3_modified_date'] = format(response['LastModified'])
	fields['s3_file_size'] = file_size
	if (len(fields['md5sum']) == 0):
		md5sum = calculate_md5sum(fields['input_file_path'])
		fields['md5sum'] = md5sum

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
		
	md5s = []
	start = datetime.datetime.now()
	print('start=', start)
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
#	print('elapsed time for checksum', computed_checksum, ':', end - start)
	value['s3_md5sum'] = computed_checksum
	update_manifest_file(out_file, od)	

# Spawns threads for calculating Google storage checksums.

def calculate_gs_checksums(od, num_threads, out_file, resume_mode):
	print('Calculating gs checksums with', num_threads, 'threads')
	start = datetime.datetime.now()
	with concurrent.futures.ThreadPoolExecutor(num_threads) as executor:	
		futures = [executor.submit(calculate_gs_checksum, value['input_file_path'], value, od, out_file, resume_mode) for key, value in od.items()]
#		print("Executing total", len(futures), "jobs")

		for idx, future in enumerate(concurrent.futures.as_completed(futures)):
			try:
				res = future.result()
#				print("Processed job", idx, "result", res)	
			except ValueError as e:
				print(e)
	end = datetime.datetime.now()
#	print('elapsed time for gs checksums:', end - start)

# Calculates crc32c value

def calculate_gs_checksum(input_file_path, value, od, out_file, resume_mode):
	global gs_crc32c
	if (input_file_path.startswith('gs://') or input_file_path.startswith('s3://')):
		#don't calculate checksum here
		return

	if (resume_mode and 'gs_crc32c' in value.keys() and len(value['gs_crc32c']) > 0):
		unsigned_int = format(struct.unpack('>I', base64.b64decode(value['gs_crc32c']))[0])
		# The separate hash, gs_crc32c, stores the unsigned_int representation of the 
		# crc32c. This is used to compute the gs key for the object.
		gs_crc32c[value['input_file_path']] = unsigned_int
		print('gs_crc32c already calculated for', value['input_file_path'], unsigned_int)
		return
		
	start = datetime.datetime.now()			
	file_bytes = open(input_file_path, 'rb').read()
	crc32c = crcmod.predefined.Crc('crc-32c')
	crc32c.update(file_bytes)
	end = datetime.datetime.now()
	base64_value = base64.b64encode(crc32c.digest()).decode('utf-8')
#	print('elapsed time for checksum:', crc32c.crcValue, base64_value, end - start)
	value['gs_crc32c'] = base64_value
	gs_crc32c[value['input_file_path']] = format(crc32c.crcValue)
	update_manifest_file(out_file, od)

# Depending on each input_file_path, figures out method for upload, and uploads to 
# Google cloud.

def upload_to_gcloud(od,  manifest_filepath, threads, chunk_size, resume_mode):    
	storage_client = storage.Client()

	tmpfilepointer = ''
	tmpfilepath = ''
	
	for key, value in od.items():
		if (resume_mode and 'gs_path' in value.keys() and value['gs_path'].startswith('gs://')):
			print("Already uploaded. Skipping", value['gs_path'])
			continue
		try:
			input_file_path = value['input_file_path']
			if (value['input_file_path'].startswith('s3://')):			
				# download the file to a temporary location
				(tmpfilepointer, tmpfilepath) = tempfile.mkstemp()
				obj = urlparse(value['input_file_path'], allow_fragments=False)			
				download_aws_key(obj.netloc, obj.path.lstrip('/'), tmpfilepath)
				calculate_gs_checksum(tmpfilepath, value, od, out_file, resume_mode)
				input_file_path = tmpfilepath
				 
			bucket_name = get_bucket_name(value)
			file = basename(value['input_file_path'])
			bucket = storage_client.bucket(bucket_name)
			blob = bucket.blob(gs_crc32c[value['input_file_path']]+ '/' + file)
			gs_path = 'gs://' + bucket_name + '/' + blob.name			
			print('Attempting to upload ' + input_file_path + ' to ' + gs_path)
			if (blob.exists()):
				blob.reload()
				add_gs_manifest_metadata(value, blob, gs_path, input_file_path)
				print("Already exists. Skipping ", 'gs://', bucket_name, '/', blob.name, sep='')
			else:
				start = datetime.datetime.now()
				# Note that gsutil automatically handles resumable transfers
				# https://cloud.google.com/storage/docs/gsutil/addlhelp/ScriptingProductionTransfers
				subprocess.check_call([
					'gsutil',
					'-o', 'GSUtil:parallel_composite_upload_threshold=%s' % ('150M'),
					'cp', input_file_path, 'gs://%s/%s' % (bucket_name, blob.name)
				])				
				end = datetime.datetime.now()
				print('elapsed time for gs upload:', end - start)
				blob = bucket.get_blob(blob.name)
				if (value['gs_crc32c'] == blob.crc32c):
					print('crc32c matches:', blob.crc32c)
					add_gs_manifest_metadata(value, blob, gs_path, input_file_path)
				else:
					print('no crc32c match', value['gs_crc32c'], '!=', blob.crc32c)
		except BadRequest as e:
			print('ERROR: problem uploading -', value['input_file_path'], e)
			value['gs_path'] = ''
			value['gs_modified_date'] = ''
			value['gs_file_size'] = ''				
		finally:
			if (tmpfilepath):
				# remove temp file
				os.remove(tmpfilepath)
				tmpfilepath = ''			
		update_manifest_file(out_file, od)	

# Given the blob object from Google Cloud, adds data into the ordered dictionary that will
# be output by the receipt manifest file

def add_gs_manifest_metadata(fields, blob, gs_path, input_file_path): 
		fields['gs_path'] = gs_path
		fields['gs_modified_date'] = format(blob.updated)
		fields['gs_file_size'] = blob.size
		if (len(fields['md5sum']) == 0):
			if (blob.md5_hash):
				fields['md5sum'] = blob.md5_hash
			else:
				md5sum = calculate_md5sum(input_file_path)
				fields['md5sum'] = md5sum

# Calculate md5sum for the path, including downloading the file if it is a cloud resource.

def calculate_md5sum(input_file_path):
	local_file = input_file_path
	tmpfilepath = ''
	
	print("Calculating md5sum for ", input_file_path)
	try:	
		if(input_file_path.startswith("gs://") or input_file_path.startswith("s3://")):
			(tmpfilepointer, tmpfilepath) = tempfile.mkstemp()
			obj = urlparse(input_file_path, allow_fragments=False)
			local_file = tmpfilepath
			
			if(local_file.startswith("gs://")):						
				download_gs_key(obj.netloc, obj.path.lstrip('/'), tmpfilepath)
			elif(local_file.startswith("s3://")):
				download_aws_key(obj.netloc, obj.path.lstrip('/'), tmpfilepath)

		if (isfile(local_file) and access(local_file, R_OK)):
			m = hashlib.md5()
			with open(local_file, 'rb') as fp:
				while True:
					data = fp.read(65536)
					if not data:
						break
					m.update(data)
		else:
			print("Not a valid path in calculate_md5sum: ", input_file_path)	
			return
	finally:
		if (tmpfilepath):
			# remove temp file
			os.remove(tmpfilepath)
		
	return m.hexdigest()

def add_blank_gs_manifest_metadata(od):
	for key, fields in od.items():
		if ('gs_crc32c' not in fields):
			fields['gs_crc32c'] = ''
		fields['gs_path'] = ''
		fields['gs_modified_date'] = ''
		fields['gs_file_size'] = ''
	
def get_bucket_name(row):
	return row['study_id'] + '--' + row['consent_group']

# This method gets called after each checksum and upload so that as much state as possible
# is written out to the receipt manifes file.

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
