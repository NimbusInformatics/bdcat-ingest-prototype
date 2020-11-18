1. Prepare Manifest File and save it with a `.tsv` extension.
   1. Sample manifest file format: `hybrid.tsv`

```  
input_file_path
sample_data/medium.txt
sample_data/small.txt
```
The first line should be `input_file_path`. Next are the paths to copy to the cloud directory. Note that the directory structure will be saved when copied. 

If you plan to run the script on multiple machines, then split the manifest file into multiple manifest files, but be sure that each manifest file starts with the line `input_file_path`.

2. Follow [Setup Instructions for Ubuntu 20.04 LTS](https://github.com/NimbusInformatics/bdcat-ingest-prototype/#setup-instructions-for-ubuntu-2004-lts).

3. Run the command

```
python process_localdir_to_aws.py --tsv hybrid.tsv --bucket avteststudy--abc --upload_threads 24 --checksum_threads 24
```

In this case, the files are uploaded to the following locations
```
s3://avteststudy--abc/sample_data/medium.txt
s3://avteststudy--abc/sample_data/small.txt
```

Note that, if order for the file paths to be valid, you would want your working directory to be the parent directory of the directory of the content. For the case of the sample manifest, you would want your working directory to be the parent directory of `sample_data`
