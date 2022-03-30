# README

Repo for experimenting with data preparation and upload for the BDCat project.

Please be sure you are always using the latest [release](https://github.com/NimbusInformatics/bdcat-ingest-prototype/releases).

For Data Custodians:
Please be sure not to share any controlled data (PII - personally identifiable information or PHI - personal health information) to unauthorized parties (including the manifest files, if they contain controlled information) including Nimbus. 

## Setup Instructions for Ubuntu 20.04 LTS

    sudo apt update
    sudo apt -y install python3-pip awscli gcc python-dev python-setuptools libffi-dev

    sudo pip3 install boto3
    sudo pip3 install google-cloud-storage
    sudo pip3 install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
    sudo pip3 install --no-cache-dir -U crcmod
    sudo pip3 install gsutil
    echo export PATH=${PATH}:$HOME/gsutil >> ~/.bashrc

    # load credentials for google cloud, if necessary
    # please note that the lines with * in front of then only need to be run if you do not already have an application credentials file.
    * gcloud auth login
	* gcloud config set pass_credentials_to_gsutil false
	gsutil config
	*find ~/.config | grep json 
	# use that path for your GOOGLE_APPLICATION_CREDENTIALS, for example, /home/boconnor/./.config/gcloud/legacy_credentials/boconnor@nimbusinformatics.com/adc.json
	export GOOGLE_APPLICATION_CREDENTIALS=<google application credentials JSON file>
	export GCLOUD_PROJECT=<your project name>

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

Run `aws configure` / set up google cloud credentials

## Running Code


First, you will want to create the working directory to be the same
name as the bucket. In this case, we will use the example `nih-nhlbi-test`-c1. This
directory, `nih-nhlbi-test-c1`, should have all the files you
want uploaded, and organized in the way you want them uploaded in the
bucket. There should not be any files in the directory that you do not
want uploaded.

After the `nih-nhlbi-test-c1` directory is set up, then cd ../
to navigate to the parent directory.

Then from that parent directory, run the following command:

`<path to>/bdcat-ingest-prototype/generate_manifest_from_file_directory.py
--directory nih-nhlbi-test-c1 --study_id <study id>
--consent_group <consent group>`

This will generate a manifest input file
(`nih-nhlbi-test-c1.tsv`). Review the file for correctness.
The blank fields will be filled in the next step.

From the same parent directory, run the next command:

`<path to>/bdcat-ingest-prototype/process_directory.py --tsv
nih-nhlbi-test-c1.tsv --gs --aws --directory
nih-nhlbi-test-c1 --bucket nih-nhlbi-test-c1`

This step may take many hours to run as it will be calculating
checksums and then finally performing a sync to the google and aws
buckets.

## Design Doc

See [20200608 - Data Ingest Brainstorming](https://docs.google.com/document/d/1bZHUKZPL7Q7onKLSdR3YBrM7oeREC54yf1g_Dpc2yVI/edit) for design information.  

## Issues

See our [Project Board](https://github.com/orgs/NimbusInformatics/projects/5) for tracking issues.

## Input manifest file format

The input manifest file is a TSV file with the following fields. See [sample.cloud.tsv](sample.cloud.tsv) for examples:

Please see [NIH Interop - Common Attributes](https://docs.google.com/spreadsheets/d/1MxfcWDXhTfFNFKsbRGjGTQkBoTirNktj04lf6L9_jmk/edit#gid=0) for more details about some of the fields.

* study\_registration - External source from which the identifier included in study\_id originates
* study\_id - required field, see naming restrictions below. Unique identifier that can be used to retrieve more information for a study
* consent_group - required field, see naming restrictions below. 
* participant\_id - Unique identifier that can be used to retrieve more information for a participant
* specimen\_id - Unique identifier that can be used to retrieve more information for a specimen
* experimental\_strategy - The experimental strategy used to generate the data file referred to by the ga4gh_drs_uri. (Based on GDC definition)
* input\_file\_path - required field. Either the local file, s3:// path, or gs:// path to be transferred
* file\_format - The format of the data, see possible values from the data_format fields in GDC.  Can use whatever values make sense for the particular implementation.
* file\_type - The type of the data, see possible values from the data_type fields in GDC.  Can use whatever values make sense for the particular implementation.

### Naming restrictions for study\_id and consent\_group
* study\_id and consent\_group should consist of only lowercase letters and numbers. 
* No special character are allowed, except for single periods (.). study\_id and consent\_group must not begin or end with a period. 
* The total number of characters for the study\_id and consent\_group combined shall not exceed 61 characters. 
* The study\_id and consent\_group combination must be globally unique.

## Output manifest file format

The output manifest file is a TSV file with the following fields.  See [sample.output.s3.manifest.tsv](sample.output.s3.manifest.tsv) for examples:


* study\_registration
* study\_id
* consent_group
* participant\_id
* specimen\_id
* experimental\_strategy
* input\_file\_path
* file\_format
* file\_type
* file\_name
* ga4gh\_drs\_uri - unique identifier for resource based on standards listed at https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_uris
* md5sum
* gs\_gs_crc32c - checksum provided by google storage in base64 format. Note that all gs\* fields will be empty if google storage was not selected
* gs\_path - path to google storage file. Note that the path includes the checksum to ensure that files are unique. It is not using the base64 format, which might lead to illegal key names, but instead the unsigned 32-bit integer value
* gs\_modified\_date - the date that the file was last uploaded or modified
* gs\_file\_size - the file size reported by google storage
* s3\_md5sum - checksum provided by aws. Note that all aws\* fields will be empty if google storage was not selected
* s3\_path - path to aws file. Note that the path includes the checksum to ensure that files are unique.
* s3\_modified\_date - the date that the file was last uploaded or modified
* s3\_file\_size - the file size reported by aws