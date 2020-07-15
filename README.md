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
   

manifest file will be located at sample.manifest.tsv

## Design Doc

See [20200608 - Data Ingest Brainstorming](https://docs.google.com/document/d/1bZHUKZPL7Q7onKLSdR3YBrM7oeREC54yf1g_Dpc2yVI/edit) for design information.  
## Issues

See our [Project Board](https://github.com/orgs/NimbusInformatics/projects/5) for tracking issues.
