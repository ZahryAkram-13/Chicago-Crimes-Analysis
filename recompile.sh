#!/bin/bash


bucket="gs://dataproc-staging-us-east1-124100932800-zukothwe"
bucket_path="${bucket}/jars/"

jars_path="target/scala-2.12/project_jars.jar"

# Recompile and reassemble the JAR
sbt clean package compile assembly
