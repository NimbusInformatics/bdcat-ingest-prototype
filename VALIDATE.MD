# Manifest File Validation

File Validation code is based on the [frictionless framework](https://framework.frictionlessdata.io/docs/guides/validating-data.html). Input manifest files are used in NHLBI BioData Catalyst data ingestion in order to provide information about the files being ingested. These files can be validated against the manifest file schema in order to confirm that the manifest files meet the requirements to be used for data ingestion. Refer to the [frictionless table schema specification](https://specs.frictionlessdata.io/table-schema/#language) for more information on defining schemas.  

NHLBI BioData Catalyst Validation Library: [bdcat_validate.py](bdcat_validate.py)

Manifest File Schema: [manifest_schema.json](manifest_schema.json)

To validate an input manifest file: 

```
python validate_manifest_file.py <manifest file>
```

# Data Dictionary File Validation (in development)

Data Dictionary Files are used to describe the format of data files. The NHLBI BioData Catalyst data dictionary format is slightly modified from the [dbGaP data dictionary format](https://www.ncbi.nlm.nih.gov/gap/docs/submissionguide/).

Data Dictionary Schema: [bdcat_data_dictionary.json](bdcat_data_dictionary.json)

To validate a data dictionary file:

```
python validate_data_dictionary_file.py <data dictionary file>
```