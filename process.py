#!/usr/bin/env python3

# python process.py --aws --tsv sample.tsv 

import argparse
#import subprocess
#import requests
import datetime
import hashlib
import csv
import sys
#import os
from os import access, R_OK
from os.path import isfile, basename
from collections import OrderedDict 

# for aws s3
import logging
import boto3
from botocore.exceptions import ClientError

# for google storage
from google.cloud import storage

def main():
	parser = argparse.ArgumentParser(description='Process TSV file.')
	parser.add_argument('--tsv', required=True, type=argparse.FileType('r'), help='tsv file')
	parser.add_argument('--gs', default=False, action='store_true', help='upload to Google Cloud')
	parser.add_argument('--aws', default=False, action='store_true', help='upload to AWS')
	parser.add_argument('--test', default=False, action='store_true', help='test mode')
	parser.add_argument('--threads', default=10, help='number of concurrent threads')
	parser.add_argument('--chunk-size', default=8 * 1024 * 1024, help='mulipart-chunk-size for uploading')

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
	read_and_verify_file(od, args) 
	checksum_files(od, args.threads)
	
	if (args.gs):
		upload_to_gcloud(od, args.threads, args.chunk_size)
	if (args.aws):
		upload_to_aws(od, args.threads, args.chunk_size)
	
	manifest_filepath = args.tsv.name
	if (manifest_filepath.endswith('.tsv')):
		manifest_filepath = manifest_filepath.replace(".tsv", ".manifest.tsv")	
	else:
		manifest_filepath += 'manifest.tsv'
	create_manifest_file(manifest_filepath, od)

def read_and_verify_file(od, args) :
	reader = csv.DictReader(args.tsv, dialect='excel-tab')
	all_files_readable = True
	all_buckets_writeable = True
	for row in reader:
		if(process_row(od, row, args.test) == False):
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
def process_row(od, row, test_mode):
	local_file = row['file_local_path']
	od[local_file] = row
	# confirm file exists and is readable
	if (isfile(local_file) and access(local_file, R_OK)):
		if (test_mode):
			print("File is readable:", local_file)			
		return True
	else:
		print("File doesn't exist or isn't readable:", local_file)
		return False
       
def verify_gs_buckets(od, test_mode):
	return
	
def verify_aws_buckets(od, test_mode):
	aws_buckets = {}
	iam = boto3.client('iam')
	sts = boto3.client('sts')
	arn = sts.get_caller_identity()['Arn']
	all_buckets_writeable = True

	for key, value in od.items(): 
		bucket_name = get_bucket_name(value)
		if (aws_bucket_writeable(bucket_name, iam, arn, aws_buckets, test_mode) == False):
			all_buckets_writeable = False
	return all_buckets_writeable

def checksum_files(od, num_threads):
	# FIXME
	return

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
# The upload_file method handles large files by splitting them into smaller chunks
# and uploading each chunk in parallel.
# See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html#uploads to
# change default transfer options
# Add mulipart upload resume option
# https://gist.github.com/holyjak/b5613c50f37865f0e3953b93c39bd61a
 
def upload_to_aws(od, threads, chunk_size): 
	aws_client = boto3.client('s3')
	transfer_config = boto3.s3.transfer.TransferConfig(multipart_chunksize=chunk_size, max_concurrency=threads, use_threads=True)  

    #TODO confirm all files with unique names???
    #TODO confirm readable cloud dir ??
    #TODO do we care if file already exists?

	for key, value in od.items(): 
		bucket_name = get_bucket_name(value)
		s3_file = basename(key)
		print('attempting to upload ', s3_file, ' to s3://', bucket_name, ' with threads=', threads, ' and chunk_size=', chunk_size, sep='')
 		
		start = datetime.datetime.now()			
		computed_checksum = calculate_s3_etag(key, chunk_size)
		end = datetime.datetime.now()
		print('elapsed time for checksum:', end - start)
		value['s3_md5sum'] = computed_checksum
		
		if (aws_key_exists(aws_client, bucket_name, key)):
			print("Already exists. Skipping", key)
			response = aws_client.head_object(Bucket=bucket_name, Key=s3_file)
			add_aws_manifest_metadata(value, response, 's3://' + bucket_name + '/' + s3_file)
		else:
			try:
				start = datetime.datetime.now()
				aws_client.upload_file(key, bucket_name, s3_file, Config=transfer_config)
				end = datetime.datetime.now()
				print('elapsed time for aws upload:', end - start)
				response = aws_client.head_object(Bucket=bucket_name, Key=s3_file)
				add_aws_manifest_metadata(value, response, 's3://' + bucket_name + '/' + s3_file)
			except ClientError as e:
				logging.error(e)
				print(e)
				value['s3_path'] = ''
				value['s3_modified_date'] = ''	 
				value['file_size'] = -1

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
				print('ERROR: bucket does not exist -', bucket_name)
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
				print('ERROR: bucket does not exist or is not writeable -', bucket_name)
				return False
		except botocore.errorfactory.NoSuchBucket as e:
			aws_buckets[bucket_name] = 0
			print('ERROR: bucket does not exist or is not writeable -', bucket_name)
			return False
		

def gs_bucket_writeable(bucket_name, test_mode):
	if (aws_buckets[bucket_name] == 1):
		return true
	elif (aws_buckets[bucket_name] == 0):
		return false
	else:
		# FIXME
		return True


def aws_key_exists(aws_client, bucket_name, key):
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(bucket_name)
	objs = list(bucket.objects.filter(Prefix=key))
	if len(objs) > 0 and objs[0].key == key:
		return True
	else:
		return False

def add_aws_manifest_metadata(fields, response, path):
	print(response)
	file_size = response['ContentLength']
	print ("size:", file_size)
	md5sum = response['ETag'][1:-1]
	print('checksum check:', fields['s3_md5sum'], ':', md5sum)
	if (fields['s3_md5sum'] == md5sum):
		print('same checksum')
	else:
		print('different checksum')
	fields['s3_path'] = path
	fields['s3_modified_date'] = response['ResponseMetadata']['HTTPHeaders']['last-modified'] 
	fields['s3_file_size'] = file_size

# code taken from https://stackoverflow.com/questions/12186993/what-is-the-algorithm-to-compute-the-amazon-s3-etag-for-a-file-larger-than-5gb#answer-19896823
# more discussion of how checksum is calculated for s3 here: https://stackoverflow.com/questions/6591047/etag-definition-changed-in-amazon-s3/28877788#28877788
def calculate_s3_etag(file_path, chunk_size):
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

# FIXME work out manifest fields when --aws and --gs both set
# FIXME set up https://cloud.google.com/storage/docs/gsutil/commands/cp#parallel-composite-uploads
# ALSO SEE https://cloud.google.com/storage/docs/working-with-big-data#composite    			
def upload_to_gcloud(od, threads, chunk_size):    
	storage_client = storage.Client()

	for key, value in od.items(): 
		bucket_name = get_bucket_name(value)
		file = basename(key)
		print('attempting to upload ' + file + ' to gs://' + bucket_name)
		bucket = storage_client.bucket(bucket_name)
		blob = bucket.blob(file)
		# Set chunk size to be same as AWS. 
		# ALSO A WORKAROUND for timeout due to slow upload speed. See https://github.com/googleapis/python-storage/issues/74
		blob.chunk_size = chunk_size
		start = datetime.datetime.now()
		blob.upload_from_filename(key)
		end = datetime.datetime.now()
		print('elapsed time for gs upload:', end - start)
		gs_path = 'gs://' + bucket_name + '/' + file
		blob = bucket.get_blob(file)		
		value['gs_path'] = gs_path
		value['file_size'] = blob.size
		value['md5sum'] = blob.md5_hash  

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
