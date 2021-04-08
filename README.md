# spp-rsi-glue-scripts
The glue script which actually runs the configured spark pipeline.

# Deploy
Glue jobs are configured and deployed using the terraform in spp-terraform.
The deploy script included in this repo adds the glue script to a specified s3 bucket, which will be referenced within spp-terraform, eg: [example](https://github.com/ONSdigital/spp-terraform/blob/b98a14b8383b7333ebe68b3011bb5fd03ac41011/aws/spp/glue.tf#L1722)

## spp_res_glue_emr_runner.py
Main script used to run the pipeline.
