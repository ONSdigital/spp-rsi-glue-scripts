#!/bin/sh

if [ -z "$1" ]
then
    echo "S3 location not provided" 1>&2
    exit 1
fi

for glue_script in ./*.py
do
    echo "Uploading $glue_script to $1"
    aws s3 cp "$glue_script" "$1"
done
