#!/usr/bin/env python3
#
# usage: process_dicom_in_gcs.py --study_id STUDY_ID --consent_group CONSENT_GROUP
#				--gcs_bucket GCS_BUCKET --gcs_user_project GCS_USER_PROJECT
#				--output_manifest_file OUTPUT_MANIFEST_FILE
#              [-h] [--test] [--resume] 
#              [--checksum_threads CHECKSUM_THREADS] [--upload_threads UPLOAD_THREADS] #              [--nifti_threads NIFTI_THREADS] [--chunk-size CHUNK_SIZE]
#
# required arguments:
#
# --study_id			study id
# --consent_group		consent_group
# --gcs_bucket			gcs_bucket
# --gcs_user_project	gcs_user_project
# --output_manifest_file	name of output manifest file that will be created by this script
#
# optional arguments:
# -h, --help            show help message
# --test                test mode: confirm inputs are valid/readable
# --resume              run process in RESUME mode, with the given manifest file
# --checksum_threads CHECKSUM_THREADS     number of concurrent threads for calculating checksums (default: number of CPUs on machine)
# --upload_threads UPLOAD_THREADS	number of concurrent threads for uploading (default: 1)
# --nifti_threads NIFTI_THREADS	number of concurrent threads for generating nifti files
# --chunk-size CHUNK_SIZE
#                       mulipart-chunk-size for uploading (default: 8 * 1024 * 1024)

#source activate nimbus--data-ingest
#time python process_dicom_in_gcs.py --gcs_bucket idc-tcia-tcga-read --gcs_user_project nimbus-fhir-test --study_id nimbusdicomtest --consent_group c1

import argparse
import signal
import sys
import os

from bdcat_ingest import get_receipt_manifest_file_pointer_for_bucket
from bdcat_ingest import download_gcs_bucket_to_localdisk
from bdcat_ingest import assign_guids
from bdcat_ingest import calculate_crc32c_threaded
from bdcat_ingest import calculate_md5sum_threaded
from bdcat_ingest import get_bucket_name
from bdcat_ingest import gcs_bucket_writeable
from bdcat_ingest import upload_from_localdisk_to_gcs_bucket
from bdcat_ingest import add_metadata_for_uploaded_gcs_bucket
from bdcat_ingest import upload_manifest_file_to_gcs_bucket


from bdcat_ingest_dicom import generate_dict_from_gcs_bucket_for_dicom
from bdcat_ingest_dicom import generate_nifti_files
from bdcat_ingest_dicom import generate_dicom_stack_metadata_files
from bdcat_ingest_dicom import update_dicom_manifest_file

out_file = ''

def main():
	args = parse_args()
	print('Script running version 2.0 on', sys.platform, 'with', os.cpu_count(), 'cpus')

	upload_gcs_bucket_name = get_bucket_name(args.study_id, args.consent_group)
	if (not gcs_bucket_writeable(upload_gcs_bucket_name)):
		print("Error: GCS Bucket Not Writeable:", upload_gcs_bucket_name)
		exit()

	# read gcs_bucket
	od = generate_dict_from_gcs_bucket_for_dicom(args.gcs_bucket, args.study_id, args.consent_group) 
	global out_file
	out_file = get_receipt_manifest_file_pointer_for_bucket(args.gcs_bucket)	
	update_dicom_manifest_file(out_file, od)

	# download files
#	download_gcs_bucket_to_localdisk(args.gcs_bucket, args.gcs_user_project)
	
	# generate DICOM manifests
	generate_dicom_stack_metadata_files(od, args.study_id, args.consent_group)
	update_dicom_manifest_file(out_file, od)
	
	# generate nifti files (threaded)
	generate_nifti_files(od, args.study_id, args.consent_group)
	update_dicom_manifest_file(out_file, od)

	# assign guids
	assign_guids(od)
	update_dicom_manifest_file(out_file, od)
	
	# reconcile checksums (threaded)
	calculate_crc32c_threaded(od, args.checksum_threads)
	update_dicom_manifest_file(out_file, od)
	calculate_md5sum_threaded(od, args.checksum_threads)
	update_dicom_manifest_file(out_file, od)
		
	# upload directory and manifest
	upload_from_localdisk_to_gcs_bucket(args.gcs_bucket + '/dicom', upload_gcs_bucket_name)
	# add metadata and check checksums for upload
	add_metadata_for_uploaded_gcs_bucket(args.gcs_bucket + '/', od, upload_gcs_bucket_name)
	update_dicom_manifest_file(out_file, od)
	upload_manifest_file_to_gcs_bucket(out_file.name, upload_gcs_bucket_name)
	print("Process complete. Manifest file located at", out_file.name)	
	exit()
	
# Parse user arguments and confirm valid arguments.

def parse_args():
	parser = argparse.ArgumentParser(description='Process DICOM in GCS.')
	parser.add_argument('--study_id', type=str, required=True, help='study_id')
	parser.add_argument('--consent_group', type=str, required=True, help='consent group')
	parser.add_argument('--gcs_bucket', type=str, required=True, help='gcs bucket')
	parser.add_argument('--gcs_user_project', type=str, required=True, help='gcs user project')
	parser.add_argument('--test', default=False, action='store_true', help='test mode: confirm inputs are valid')
	parser.add_argument('--resume', default=False, action='store_true', help='run process in RESUME mode, with the given manifest file')
	parser.add_argument('--checksum_threads', type=int, default=os.cpu_count(), help='number of concurrent checksum threads (default: number of CPUs on machine)')
	parser.add_argument('--upload_threads', type=int, default=1, help='number of concurrent upload threads (default: 1)')
	parser.add_argument('--chunk-size', type=int, default=8 * 1024 * 1024, help='mulipart-chunk-size for uploading (default: 8 * 1024 * 1024)')
	
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
