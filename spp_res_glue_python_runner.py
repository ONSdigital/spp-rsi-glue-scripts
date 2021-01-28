# coding=utf-8

from __future__ import unicode_literals

import base64
import json
import sys

from spp.engine.pipeline import construct_pipeline

from awsglue.utils import getResolvedOptions
from es_aws_functions import general_functions

args = getResolvedOptions(sys.argv, ["config", "crawler"])
config_str = base64.b64decode(args["config"].encode("ascii")).decode("ascii")
config = json.loads(config_str)
crawler = args["crawler"]
survey = config["survey"]
environment = config["pipeline"]["environment"]
run_id = config["pipeline"]["run_id"]

logger = general_functions.get_logger(survey, "spp-results-python-pipeline", environment, run_id)

try:
    logger.info("Config variables loaded.")
    pipeline = construct_pipeline(config["pipeline"], logger=logger)
    logger.info("Running pipeline {}, run {}".format(pipeline.name, config["run_id"]))
    pipeline.run(
        crawler_name=crawler
    )
except Exception:
    logger.exception("Exception occurred in pipeline.")
