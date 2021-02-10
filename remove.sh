#!/bin/bash

if [ ! $1 ]; then
    echo "S3 location not provided"
    exit 1
fi
s3location=$1
for VARIABLE in glue_rsi_ingest.py spp_res_glue_emr_runner.py
do
./remove-glue-scripts.sh $VARIABLE $s3location
done
