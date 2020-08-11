# README

Repo for experimenting with data preparation and upload for the BDCat project.

## Setup Instructions for Ubuntu 20.04 LTS

In version 1.0, files on cloud services are downloaded before having their checksum 
calculated and uploaded. Please ensure that your VM has enough disk storage space for 
the largest file in your manifest file.


    sudo apt update
    sudo apt -y install python3-pip awscli gcc python-dev python-setuptools libffi-dev

    sudo pip3 install boto3
    sudo pip3 install google-cloud-storage
    sudo pip3 install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
    sudo pip3 install --no-cache-dir -U crcmod
    sudo pip3 install gsutil
    echo export PATH=${PATH}:$HOME/gsutil >> ~/.bashrc

    # load credentials for google cloud, if necessary
    export GOOGLE_APPLICATION_CREDENTIALS="<path to credentials .json file>"
    gsutil config

    # load credentials for aws, if necesary
    aws configure

## Setup Instructions for MacOS

    conda create -n python_3_7_4_20200619 python=3.7.4
    conda create --name nimbus--data-ingest python=3.7.4
    conda activate nimbus--data-ingest
    pip install awscli
    pip install boto3
    pip install google-cloud-storage
    pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
    conda activate nimbus--data-ingest
	
1. Create large file called big_binary.MOV in current directory (should be > 8 MB)

2. Run `aws configure` / set up google cloud credentials

3. For testing, update study\_id fields in sample.tsv. Then create aws/gs s3 buckets with the name `<study_id>--<consent_code>`

## Running Code


    python3 process.py --aws --tsv sample.multifile.tsv 
    or
    python3 process.py --gs --tsv sample.multifile.tsv 
   

The output manifest file will be located at sample.mulifile.<timestamp>manifest.tsv

## Design Doc

See [20200608 - Data Ingest Brainstorming](https://docs.google.com/document/d/1bZHUKZPL7Q7onKLSdR3YBrM7oeREC54yf1g_Dpc2yVI/edit) for design information.  

## Issues

See our [Project Board](https://github.com/orgs/NimbusInformatics/projects/5) for tracking issues.

## Input manifest file format

The input manifest file is a TSV file with the following fields. See [sample.multifile.tsv](https://raw.githubusercontent.com/NimbusInformatics/bdcat-ingest-prototype/master/sample.multifile.tsv) for examples:

* study\_id - required field, see naming restrictions below
* dbgap\_study\_id
* consent_group - required field, see naming restrictions below
* study\_id
* specimen\_id
* experimental\_strategy
* input\_file\_path - required field. Either the local file, s3:// path, or gs:// path to be transferred

### Naming restrictions for study\_id and consent\_group
* study\_id and consent\_group should consist of only lowercase letters and numbers. 
* No special character are allowed, except for single hyphens (-). study\_id and consent\_group must not begin or end with a hyphen. 
* The total number of characters for the study\_id and consent\_group combined shall not exceed 61 characters. 
* The study\_id and consent\_group combination must be globally unique.

## Output manifest file format

The output manifest file is a TSV file with the following fields:

* study\_id
* dbgap\_study\_id
* consent_group
* study\_id
* specimen\_id
* experimental\_strategy
* input\_file\_path
* drs\_uri - unique identifier for resource based on standards listed at [https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_uris] (https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_uris)
* md5sum
* gs\_gs_crc32c - checksum provided by google storage in base64 format. Note that all gs\* fields will be empty if google storage was not selected
* gs\_path - path to google storage file. Note that the path includes the checksum to ensure that files are unique. It is not using the base64 format, which might lead to illegal key names, but instead the unsigned 32-bit integer value
* gs\_modified\_date - the date that the file was last uploaded or modified
* gs\_file\_size - the file size reported by google storage
* s3\_md5sum - checksum provided by aws. Note that all aws\* fields will be empty if google storage was not selected
* s3\_path - path to aws file. Note that the path includes the checksum to ensure that files are unique.
* s3\_modified\_date - the date that the file was last uploaded or modified
* s3\_file\_size - the file size reported by aws