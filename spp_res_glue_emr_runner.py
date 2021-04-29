import boto3
import importlib
import immutables
import json
import pyspark.sql
import spp_logger
import sys
from awsglue.utils import getResolvedOptions


bpm_queue_url = None
component = "spp-results-emr-glue-runner"
environment = "unconfigured"
logger = None
pipeline = "unconfigured"
region = "eu-west-2"
run_id = None


def send_status(status, module_name):
    bpm_message = {
        "bpm_id": run_id,
        "status": {
            "step_name": module_name,
            "message": {},
            "state": status
        },
    }
    bpm_message = json.dumps(bpm_message)
    sqs = boto3.client("sqs", region_name=region)
    sqs.send_message(
        QueueUrl=bpm_queue_url,
        MessageBody=bpm_message,
        MessageGroupId=run_id)


def get_logger(survey, module_name, environment, run_id, log_level="INFO"):
    # set the logger context attributes
    main_context = immutables.Map(
        log_correlation_id=run_id,
        log_correlation_type=survey,
        log_level=log_level
    )
    config = spp_logger.SPPLoggerConfig(
        service="Results",
        component=module_name,
        environment=environment,
        deployment=environment
    )

    return spp_logger.SPPLogger(
        name="my_logger",
        config=config,
        context=main_context,
    )


try:
    args = getResolvedOptions(sys.argv, [
        "bpm_queue_url",
        "environment",
        "pipeline",
        "run_id",
        "snapshot_location"
    ])
    bpm_queue_url = args["bpm_queue_url"]
    environment = args["environment"]
    pipeline = args["pipeline"]
    run_id = args["run_id"]
    snapshot_location = args["snapshot_location"]

    s3 = boto3.resource("s3", region_name=region)
    config = json.load(
        s3.Object(
            f"spp-res-{environment}-config",
            f"{pipeline}.json"
        ).get()["Body"]
    )

    methods = config["methods"]
    run_id_column = config.get("run_id_column", "run_id")
    logger = get_logger(
        pipeline,
        component,
        environment,
        run_id
    )

    logger.info(
        "Running pipeline %s with snapshot %s",
        pipeline,
        snapshot_location
    )
    # Set up extra params for ingest provided at runtime
    extra_ingest_params = {
        "run_id": run_id,
        "snapshot_location": snapshot_location
    }
    ingest_params = methods[0].get("params", {})
    ingest_params.update(extra_ingest_params)
    methods[0]["params"] = ingest_params

    spark = (
        pyspark.sql.SparkSession.builder.enableHiveSupport()
        .appName(pipeline)
        .getOrCreate()
    )
    spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    send_status("IN PROGRESS", pipeline)
    # The first method is expected to get its data location at runtime whereas
    # subsequent methods must get their input from the output table of the
    # previous method
    data_location = None

    for method in methods:
        send_status("IN PROGRESS", method["name"])

        logger.info("Starting method %s.%s", method["module"], method["name"])
        method_params = method.get("params", {})
        if method.get("provide_session"):
            method_params["spark"] = spark

        if data_location is not None:
            # Contains location of previous method's output
            method_params["df"] = spark.table(data_location).filter(
                pyspark.sql.functions.col(run_id_column) == run_id)

        module = importlib.import_module(method["module"])

        output = getattr(module, method["name"])(**method_params)

        if output.count() == 0:
            raise RuntimeError(
                f"{method['module']}.{method['name']} returned 0 rows")

        data_location = method["data_target"]
        # We need to select the relevant columns from the output
        # to support differing column orders and so that we get
        # only the columns we want in our output tables
        (output.select(spark.table(data_location).columns)
            .write.insertInto(data_location, overwrite=True))

        logger.info("Finished method %s.%s", method["module"], method["name"])

    send_status("FINISHED", pipeline)

except Exception:
    if logger is None:
        logger = get_logger(
            pipeline,
            component,
            environment,
            run_id
        )

    logger.exception("Exception occurred in glue job")
    if bpm_queue_url is not None:
        send_status("ERROR", pipeline)

    sys.exit(1)
