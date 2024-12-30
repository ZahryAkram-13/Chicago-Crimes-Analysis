#!/bin/bash

project="big-data-444917"
bucket="gs://dataproc-staging-us-east1-124100932800-zukothwe"
bucket_path="${bucket}/jars/"

jars_path="target/scala-2.12/project_jars.jar"


gcloud auth login
gcloud config set project $project
gsutil cp  "$jars_path" "$bucket_path"