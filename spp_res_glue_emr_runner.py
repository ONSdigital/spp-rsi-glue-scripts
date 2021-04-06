import base64
import json
import sys

from spp.engine.pipeline import construct_pipeline

from awsglue.utils import getResolvedOptions
from es_aws_functions import general_functions

args = getResolvedOptions(sys.argv, ["config"])
config_str = base64.b64decode(args["config"].encode("ascii")).decode("ascii")
config = json.loads(config_str)
survey = config["survey"]
environment = config["pipeline"]["environment"]
run_id = config["pipeline"]["run_id"]
# Some parameters for ingest are only known at runtime, however the config
# is the only mechanism we have to pass them to the pipeline
ingest_params = config["pipeline"]["methods"][0].get("params", {})
ingest_params["run_id"] = run_id
ingest_params["snapshot_location"] =\
    config["snapshot_location"]

config["pipeline"]["methods"][0]["params"] = ingest_params
logger = general_functions.get_logger(survey,
                                      "spp-results-emr-pipeline",
                                      environment, run_id)

try:
    logger.info("Config variables loaded.")
    pipeline = construct_pipeline(config["pipeline"], logger=logger)
    logger.info("Running pipeline {}, run {}".format(pipeline.name, run_id))
    if not pipeline.run():
        logger.error("Failed running pipeline")
        sys.exit(1)

except Exception:
    logger.exception("Exception occurred in pipeline")
    sys.exit(1)
