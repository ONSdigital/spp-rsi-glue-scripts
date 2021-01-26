# spp-rsi-glue-scripts
A collection of python scripts used within the RSI pipeline.

# Deploy
Glue jobs are configured and deployed using the terraform in spp-terraform.
The deploy script included in this repo adds the three glue scripts to a specified s3 bucket, which will be referenced within spp-terraform, eg: [example](https://github.com/ONSdigital/spp-terraform/blob/b98a14b8383b7333ebe68b3011bb5fd03ac41011/aws/spp/glue.tf#L1722)
## glue_rsi_ingest.py
Used to ingest snapshot data for the pipeline to use

## spp_res_glue_emr_runner.py
Main script used by RSI, in conjunction with the spp engine runs a series of spark jobs in turn.

## spp_res_glue_python_runner.py 
As above but for pyshell jobs