#!/bin/bash

if [ ! $1 ]; then
    echo "S3 location not provided"
    exit 1
fi
if [ ! $2 ]; then
    echo "File not provided"
    exit 1
fi
s3location=$1
file=$2

echo "Adding $file to $s3location"
#aws s3 cp /$file $s3location