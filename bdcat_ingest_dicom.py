import base64
import csv
import datetime
import os
from os.path import isfile, basename
import subprocess
import threading

from collections import OrderedDict 

from bdcat_ingest import generate_dict_from_gcs_bucket
from bdcat_ingest import get_file_metadata_for_file_path

import pydicom
import SimpleITK as sitk

def generate_dict_from_gcs_bucket_for_dicom(gcs_bucket, study_id, consent_group):
	od = generate_dict_from_gcs_bucket(gcs_bucket, study_id, consent_group)
	for key, row in od.items():
		# these are additional fields for dicom files
		row['intermediate_file_name'] = row['file_name']
		row['dicom_stack'] = ''	
		row['nifti_file_name'] = ''	
		row['metadata_file_name'] = ''					
		# dicom metadata fields
		row['participant_id'] = ''
		row['SeriesDescription'] = '' 
		row['StudyInstanceUID'] = '' 
		row['SeriesInstanceUID'] = '' 
		row['StudyDate'] = '' 
		row['ConvolutionKernel'] = '' 
		row['Manufacturer'] = '' 
		row['ManufacturerModelName'] = '' 
		row['SliceThickness'] = '' 
		row['ReconstructionDiameter'] = '' 


		curr_file_name = row['file_name']
		if (curr_file_name.startswith('gs://')):
			row['intermediate_file_name'] = row['file_name']
			row['intermediate_file_name'] = row['intermediate_file_name'].replace('gs://' , '', 1)
			curr_file_name = row['intermediate_file_name']
		elif (curr_file_name.startswith('s3://')):
			row['intermediate_file_name'] = row['file_name']
			row['intermediate_file_name'] = row['intermediate_file_name'].replace('s3://', '', 1)
			curr_file_name = row['intermediate_file_name']		
		file_name, file_extension = os.path.splitext(curr_file_name)
		if (file_extension == '.dcm'):
			row['file_type'] = 'DICOM'
			# the parent directory is the DICOM stack
			row['dicom_stack'] = os.path.dirname(curr_file_name)			
			row['nifti_file_name'] = row['dicom_stack'] + '.nii'
			row['metadata_file_name'] = row['dicom_stack'] + '.tsv'
	return od		

def generate_nifti_files(od, study_id, consent_group):
	dicom_dictionary = get_dicom_stacks_for_value(od, 'nifti_file_name')
	for dicom_stack, nifti_file_name in dicom_dictionary.items():
		# FIXME multithread
		generate_nifti_file(dicom_stack, nifti_file_name)
		nifti_row = get_file_metadata_for_file_path(nifti_file_name, study_id, consent_group)
		nifti_row['file_type'] = 'NIFTI'
		nifti_row['dicom_stack'] = dicom_stack
		# FIXME add more metadata
		od[nifti_file_name] = nifti_row
		
def get_dicom_stacks_for_value(od, value):
	dicom_dictionary = OrderedDict()

	for key, row in od.items():
		if ('dicom_stack' in row and row['dicom_stack'] != '' and value in row and row[value] != ''):
			dicom_dictionary[row['dicom_stack']] = row[value]
	return dicom_dictionary

def get_dicom_image_files_for_stack(od, dicom_stack):
	dicom_files = []
#	print ('in get_dicom_image_files_for_stack, dicom_stack =', dicom_stack)
	for key, row in od.items():
		if ('dicom_stack' in row and row['dicom_stack'] == dicom_stack and row['file_type'] == 'DICOM'):
			dicom_file_name = row['file_name']
			if ('intermediate_file_name' in row and row['intermediate_file_name'] != ''):
				dicom_file_name = row['intermediate_file_name']
			dicom_files.append(dicom_file_name)
#	print('dicom_files: ', dicom_files)
	return dicom_files
	
def generate_nifti_file(dicom_stack, nifti_file_name):
	print('Generating', nifti_file_name)
	compressOutput = bool(True)
	dicom_reader = sitk.ImageSeriesReader()
	dicom_file_names = dicom_reader.GetGDCMSeriesFileNames(dicom_stack)
	print ('in generate_nifti_file, dicom_file_names =', dicom_file_names)
	dicom_reader.SetFileNames(dicom_file_names)
	output_image = dicom_reader.Execute()
	sitk.WriteImage(output_image, nifti_file_name, compressOutput)

def generate_dicom_stack_metadata_files(od, study_id, consent_group):
	dicom_dictionary = get_dicom_stacks_for_value(od, 'metadata_file_name')

	for dicom_stack, metadata_file_name in dicom_dictionary.items():
		# FIXME multithread
		generate_dicom_stack_metadata_file(od, dicom_stack, metadata_file_name, study_id, consent_group)

def generate_dicom_stack_metadata_file(od, dicom_stack, metadata_file_name, study_id, consent_group):
	dicom_file_names = get_dicom_image_files_for_stack(od, dicom_stack)
	keys = ['dicom_file_name']
	value_rows = []

	# read once to get the set of keys used in the image files
	for dicom_file_name in dicom_file_names:
		print ('in generate_dicom_stack_metadata_file, dicom_file_name = ', dicom_file_name)
		reader = sitk.ImageFileReader()
		reader.SetFileName(dicom_file_name)
	#	reader.LoadPrivateTagsOn()
		reader.ReadImageInformation()
		
		for key in reader.GetMetaDataKeys():
			if (key not in keys):
				keys.append(key)

	for dicom_file_name in dicom_file_names:
		reader = sitk.ImageFileReader()
		reader.SetFileName(dicom_file_name)
	#	reader.LoadPrivateTagsOn()
		reader.ReadImageInformation()
		values = []
		values.append(basename(dicom_file_name))
		temp_row = {}
		
		for key in keys:
			if (key != 'dicom_file_name'):
				value = reader.GetMetaData(key)
				values.append(value)
				# This updates the row for the dicom stack metadata file. The assumption is that
				# this value is the same across all files in the stack.
				if (key == '0010|0020' and value != ''):
					temp_row['participant_id'] = value
				if ((key == '0008|103E' or key == '0008|103e') and value != ''):
					temp_row['SeriesDescription'] = value 
				if ((key == '0020|000D' or key == '0020|000d') and value != ''):
					temp_row['StudyInstanceUID'] = value 
				if ((key == '0020|000E' or key == '0020|000e') and value != ''):
					temp_row['SeriesInstanceUID'] = value 
				if (key == '0008|0020' and value != ''):
					temp_row['StudyDate'] = value 
				if (key == '0018|1210' and value != ''):
					temp_row['ConvolutionKernel'] = value 
				if (key == '0008|0070' and value != ''):
					temp_row['Manufacturer'] = value 
				if (key == '0008|1090' and value != ''):
					temp_row['ManufacturerModelName'] = value 
				if (key == '0018|0050' and value != ''):
					temp_row['SliceThickness'] = value 
				if (key == '0018|1100' and value != ''):
					temp_row['ReconstructionDiameter'] = value 	
		value_rows.append(values)		       

	with open(metadata_file_name, "w") as outfile:
		tsv_writer = csv.writer(outfile, delimiter='\t')
		tsv_writer.writerow(keys)
		for values in value_rows:
			tsv_writer.writerow(values)

	metadata_row = get_dicom_file_metadata_for_file_path(metadata_file_name, study_id, consent_group)
	metadata_row['file_type'] = 'TSV'
	metadata_row['dicom_stack'] = dicom_stack
	od[metadata_file_name] = metadata_row
	
	for key, value in temp_row.items():
		metadata_row[key] = value
	

# Generates one dicom metadata tsv file per dicom file
def generate_dicom_metadata_files(od, study_id, consent_group):
	files_added = []

	for key, row in od.items():
		if (row['metadata_file_name'] != ''):
			dicom_file_name = row['file_name']
			if ('intermediate_file_name' in row and row['intermediate_file_name'] != ''):
				dicom_file_name = row['intermediate_file_name']
			generate_dicom_metadata_file(row, dicom_file_name, row['metadata_file_name'])
			files_added.append(row['metadata_file_name'])

	# Add the newly created file to the output manifest dictionary			
	for metadata_file_name in files_added:
		metadata_row = get_dicom_file_metadata_for_file_path(metadata_file_name, study_id, consent_group)
		metadata_row['file_type'] = 'TSV'
		od[metadata_file_name] = metadata_row
	
# code reference: https://simpleitk.readthedocs.io/en/master/link_DicomImagePrintTags_docs.html
# Generates a dicom metadata tsv file for a  dicom image file	
def generate_dicom_metadata_file(row, dicom_file_name, metadata_file_name):
	reader = sitk.ImageFileReader()
	reader.SetFileName(dicom_file_name)
#	reader.LoadPrivateTagsOn()
	reader.ReadImageInformation()

	keys = []
	values = []

	keys.append('dicom_file_name')
	values.append(dicom_file_name)

	for key in reader.GetMetaDataKeys():
		value = reader.GetMetaData(key)
		keys.append(key)
		values.append(value)
		if (key == '0010|0020'):
			row['participant_id'] = value
		if (key == '0008|103E' or key == '0008|103e'):
			row['SeriesDescription'] = value 
		if (key == '0020|000D' or key == '0020|000d'):
			row['StudyInstanceUID'] = value 
		if (key == '0020|000E' or key == '0020|000e'):
			row['SeriesInstanceUID'] = value 
		if (key == '0008|0020'):
			row['StudyDate'] = value 
		if (key == '0018|1210'):
			row['ConvolutionKernel'] = value 
		if (key == '0008|0070'):
			row['Manufacturer'] = value 
		if (key == '0008|1090'):
			row['ManufacturerModelName'] = value 
		if (key == '0018|0050'):
			row['SliceThickness'] = value 
		if (key == '0018|1100'):
			row['ReconstructionDiameter'] = value 			       

	with open(metadata_file_name, "w") as outfile:
		tsv_writer = csv.writer(outfile, delimiter='\t')
		tsv_writer.writerow(keys)
		tsv_writer.writerow(values)


def get_dicom_file_metadata_for_file_path(metadata_file_name, study_id, consent_group):
	row = get_file_metadata_for_file_path(metadata_file_name, study_id, consent_group)
	# these are additional fields for dicom files
	row['intermediate_file_name'] = row['file_name']
	row['dicom_stack'] = ''	
	row['nifti_file_name'] = ''	
	row['metadata_file_name'] = ''					
	# dicom metadata fields
	row['participant_id'] = ''
	row['SeriesDescription'] = '' 
	row['StudyInstanceUID'] = '' 
	row['SeriesInstanceUID'] = '' 
	row['StudyDate'] = '' 
	row['ConvolutionKernel'] = '' 
	row['Manufacturer'] = '' 
	row['ManufacturerModelName'] = '' 
	row['SliceThickness'] = '' 
	row['ReconstructionDiameter'] = '' 	
	return row

def update_dicom_manifest_file(f, od):
	with threading.Lock():
		# start from beginning of file
		f.seek(0)
		f.truncate()
	
		isfirstrow = True
		tsv_writer = csv.writer(f, delimiter='\t')
		for key, row in od.items():
			if (isfirstrow):
				# print header row
				tsv_writer.writerow(row.keys())
				isfirstrow = False
			if ('file_type' in row and row['file_type'] != 'DICOM'): 
				tsv_writer.writerow(row.values())
	# we don't close the file until the end of the operation		

