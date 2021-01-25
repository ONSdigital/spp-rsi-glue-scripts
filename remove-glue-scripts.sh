#!/bin/bash

if [ ! $1 ]; then
    echo "S3 location not provided"
    exit 1
fi
if [ ! $2 ]; then
    echo "File not provided"
    exit 1
fi
s3location=$2
file=$1

echo "Removing $file to $s3location"

aws s3 rm $s3location$file
