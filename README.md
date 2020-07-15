# README

Repo for experimenting with data preparation and upload for the BDCat project.

## Setup

    conda create -n python_3_7_4_20200619 python=3.7.4
    conda create --name nimbus--data-ingest python=3.7.4
    conda activate nimbus--data-ingest
    pip install awscli
    pip install boto3
    pip install google-cloud-storage
	pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
	
1. Create large file called big_binary.MOV in current directory (should be > 8 MB)

2. Run `aws configure` / set up google cloud credentials

3. For testing, update study\_id fields in sample.tsv. Then create aws/gs s3 buckets with the name `<study_id>-<consent_code>`

## Running Code


    conda activate nimbus--data-ingest
    python process.py --aws --tsv sample.tsv 
    or
    python process.py --gs --tsv sample.tsv 
   

The output manifest file will be located at sample.manifest.tsv

## Design Doc

See [20200608 - Data Ingest Brainstorming](https://docs.google.com/document/d/1bZHUKZPL7Q7onKLSdR3YBrM7oeREC54yf1g_Dpc2yVI/edit) for design information.  
## Issues

See our [Project Board](https://github.com/orgs/NimbusInformatics/projects/5) for tracking issues.

## Input manifest file format

The input manifest file is a TSV file with the following fields:

* study\_id
* dbgap\_study\_id
* consent\_code
* participant\_id
* specimen\_id
* experimental\_strategy
* file\_local\_path

## Output manifest file format

The output manifest file is a TSV file with the following fields:

* study\_id
* dbgap\_study\_id
* consent\_code
* participant\_id
* specimen\_id
* experimental\_strategy
* file\_local\_path
* gs\_md5sum - checksum provided by google storage. Note that all gs\* fields will be empty if google storage was not selected
* gs\_path - path to google storage file. Note that the path includes the checksum to ensure that files are unique.
* gs\_modified\_date - the date that the file was last uploaded or modified
* gs\_file\_size - the file size reported by google storage
* s3\_md5sum - checksum provided by aws. Note that all aws\* fields will be empty if google storage was not selected
* s3\_path - path to aws file. Note that the path includes the checksum to ensure that files are unique.
* s3\_modified\_date - the date that the file was last uploaded or modified
* s3\_file\_size - the file size reported by aws