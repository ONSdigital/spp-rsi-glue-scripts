# coding=utf-8

from __future__ import unicode_literals

import base64
import json
import sys

from spp.engine.pipeline import construct_pipeline

from awsglue.utils import getResolvedOptions
from es_aws_functions import general_functions

current_module = "spp-res_glu_emr_job_script"
args = getResolvedOptions(sys.argv, ["config", "crawler"])
config_str = base64.b64decode(args["config"].encode("ascii")).decode("ascii")
config = json.loads(config_str)
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
        crawler_name=crawler
    )
except Exception as e:
    logger.error("Error constructing and/or running pipeline: ", e)
    raise e
