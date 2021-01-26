# coding=utf-8

"""
Created on Tue Nov 19 14:30:00 2019
@author: DPM

Version History:
    updated for spp-engine testing on 22/11/2019.
    update for spp-terraform on 11/01/2020.
"""


from __future__ import unicode_literals

import json
import sys

from spp.engine.pipeline import construct_pipeline

from awsglue.utils import getResolvedOptions
from es_aws_functions import general_functions

current_module = "spp-res_glu_emr_job_script"
args = getResolvedOptions(sys.argv, ["config", "crawler"])
config_parameters_string = (
    (args["config"]).replace("'", '"').replace("True", "true").replace("False", "false")
)
config = json.loads(config_parameters_string)
crawler = args["crawler"]
environment = config["pipeline"]["environment"]
run_id = config["pipeline"]["run_id"]
survey = config["survey"]
config = config["pipeline"]
try:
    logger = general_functions.get_logger(survey, current_module, environment, run_id)
except Exception as e:
    raise e

try:
    logger.info("Config variables loaded.")
    pipeline = construct_pipeline(config, survey)
    logger.info("Running pipeline {}, run {}".format(pipeline.name, config["run_id"]))
    pipeline.run(
        platform=config["platform"],
        crawler_name=crawler,
        survey=survey,
        environment=environment,
        run_id=run_id,
    )
except Exception as e:
    logger.error("Error constructing and/or running pipeline: ", e)
    raise e
