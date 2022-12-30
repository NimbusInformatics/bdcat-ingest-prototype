from frictionless import validate
from frictionless import Schema

def validate_file(file_path, schema_path):
	report = validate(file_path, schema=schema_path)
	print(report)

def validate_manifest_file(file_path):
	validate_file(file_path, 'manifest_schema.json')

def validate_data_dictionary_file(file_path):
	validate_file(file_path, 'bdcat_data_dictionary.json')

# https://specs.frictionlessdata.io/table-schema/#language
def generate_skeleton_schema_from_file(file_path, schema_path):
	schema = Schema.describe(file_path)
	schema.to_json(schema_path) 

	

