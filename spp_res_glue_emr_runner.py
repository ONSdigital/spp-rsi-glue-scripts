import base64
import importlib
import json
import pyspark.sql
import sys
from awsglue.utils import getResolvedOptions
from es_aws_functions import aws_functions
from es_aws_functions import general_functions

args = getResolvedOptions(sys.argv, ["config"])
config_str = base64.b64decode(args["config"].encode("ascii")).decode("ascii")
config = json.loads(config_str)
survey = config["survey"]
environment = config["pipeline"]["environment"]
run_id = config["run_id"]
name = config["name"]
bpm_queue_url = config.get("bpm_queue_url")
logger = general_functions.get_logger(survey,
                                      "spp-results-emr-pipeline",
                                      environment, run_id)


def send_status(status, module_name, current_step_num=None):
    if bpm_queue_url is None:
        return

    aws_functions.send_bpm_status(
        bpm_queue_url,
        module_name,
        status,
        run_id,
        survey="RSI",
        current_step_num=current_step_num,
        total_steps=len(config["methods"]),
    )


logger.info("Running pipeline %s", name)

try:
    # Set up extra params for ingest provided at runtime
    config["pipeline"]["methods"][0]["params"]["run_id"] = run_id
    config["pipeline"]["methods"][0]["params"]["snapshot_location"] =\
        config["snapshot_location"]

    methods = config["methods"]
    logger.debug("Starting spark Session for %s", name)
    spark = (
        pyspark.sql.SparkSession.builder.enableHiveSupport()
        .appName(name)
        .getOrCreate()
    )
    spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    send_status("IN PROGRESS", name)

    for method_num, method in enumerate(methods):
        # method_num is 0-indexed but we probably want step numbers
        # to be 1-indexed
        step_num = method_num + 1
        send_status(
            "IN PROGRESS", method["name"], current_step_num=step_num
        )

        if method["provide_session"]:
            method["params"]["spark"] = spark

        data_source = method.get("data_source")
        if data_source is not None:
            logger.debug("Retrieving data from %r", data_source)
            df = spark.table(data_source)
            df = df.filter(df.run_id == run_id)
            if df.count() == 0:
                raise RuntimeError(f"Found no rows for run id {run_id}")
            method["params"]["df"] = df

        module = importlib.import_module(method["module"])
        output = getattr(module, method["name"])(**method["params"])

        data_target = method.get("data_target")
        if data_target is not None:
            # We need to select the relevant columns from the output
            # to support differing column orders and so that we get
            # only the columns we want in our output tables
            output = output.select(spark.table(data_target).columns)
            output.write.insertInto(data_target, overwrite=True)

        send_status("DONE", method["name"], current_step_num=step_num)
        logger.info("Method Finished: %s", method["name"])

    send_status("DONE", name)

except Exception:
    logger.exception("Exception occurred in pipeline")
    send_status("ERROR", name)
    sys.exit(1)
