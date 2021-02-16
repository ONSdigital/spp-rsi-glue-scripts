import base64
import json
import pandas as pd
import boto3
from io import StringIO
from awsglue.utils import getResolvedOptions
import time
import os
import sys

extension_types = {".json": "application/json", ".csv": "text/csv"}

region = "eu-west-2"


def get_from_file():
    config_dir = [f for f in os.listdir("./") if f.startswith("glue-python-libs-")][0]
    filepath = "./" + config_dir + "/" + os.listdir("./" + config_dir)[0]
    with open(filepath, "r") as f:
        return f.read()


def read_from_s3(bucket_name, file_name, file_prefix="", file_extension=".json"):
    """
    Given the name of the bucket and the filename(key), this function will
    return a file. File is JSON format.
    :param bucket_name: Name of the S3 bucket - Type: String
    :param file_name: Name of the file - Type: String
    :param file_prefix: Optional, run id to be added as file name prefix - Type: String
    :param file_extension: The file extension that the submitted file should have.
    :return: input_file: The JSON file in S3 - Type: String
    """
    s3 = boto3.resource("s3", region_name=region)
    full_file_name = file_name
    if len(file_prefix) > 0:
        full_file_name = file_prefix + full_file_name
    try:
        s3_object = s3.Object(bucket_name, full_file_name)
        input_file = s3_object.get()["Body"].read().decode("UTF-8")
    except Exception as e:
        raise Exception(f"Could not find s3://{bucket_name}/{full_file_name}.{type(e)}")
    return input_file


def save_dataframe_to_csv(
    dataframe, bucket_name, file_name, file_prefix="", file_extension=".csv"
):
    """
    This function takes a Dataframe and stores it in a specific bucket.
    :param dataframe: The Dataframe you wish to save - Type: Dataframe.
    :param bucket_name: Name of the bucket you wish to save the csv into - Type: String.
    :param file_name: The name given to the CSV - Type: String.
    :param file_prefix: Optional, run id to be added as file name prefix - Type: String
    :param file_extension: The file extension that the submitted file should have.
    :return: None
    """
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    data = csv_buffer.getvalue()

    save_to_s3(bucket_name, file_name, data, file_prefix, file_extension)


def save_to_s3(
    bucket_name, output_file_name, output_data, file_prefix="", file_extension=".json"
):
    """
    This function uploads a specified set of data to the s3 bucket under the given name.
    :param bucket_name: Name of the bucket you wish to upload too - Type: String.
    :param output_file_name: Name you want the file to be called on s3 - Type: String.
    :param output_data: The data that you wish to upload to s3 - Type: JSON.
    :param file_prefix: Optional, run id to be added as file name prefix - Type: String
    :param file_extension: The file extension that the submitted file should have.
    :return: None
    """
    s3 = boto3.resource("s3", region_name=region)

    full_file_name = output_file_name + file_extension
    if len(file_prefix) > 0:
        full_file_name = file_prefix + full_file_name

    s3.Object(bucket_name, full_file_name).put(
        Body=output_data, ContentType=extension_types[file_extension]
    )


def do_query(client, query, config, execution_context=False):
    """
    Performs Athena queries and returns their result
    :param client - boto3 client: Athena Client
    :param query - String: SQL query to execute
    :param config - Json String: Config for query, contains OutputLocation
    :param execution_context - Json String: Config for query,
                                contains database name to use.
    :return result - Json String: Results of that query
    """
    # First query to execute is the create database.
    # execution context doesnt exist until that point
    if execution_context is not False:
        execution = client.start_query_execution(
            QueryString=query,
            ResultConfiguration=config,
            QueryExecutionContext=execution_context,
        )
    else:
        execution = client.start_query_execution(
            QueryString=query, ResultConfiguration=config
        )
    execution_id = execution["QueryExecutionId"]

    # Wait for query to complete
    max_execution = 20
    state = "RUNNING"
    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution = max_execution - 1
        # Get query status
        response = client.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]

            # If anything but succeeeded/failed, go back around the loop
            if state == "FAILED":
                return False
            elif state == "SUCCEEDED":
                # On success, return the results of the query
                return client.get_query_results(QueryExecutionId=execution_id)

        time.sleep(1)
    return False


def ingest(config, snapshot_location_bucket, snapshot_location_key, run_id):
    survey_nodes = read_from_s3(snapshot_location_bucket, snapshot_location_key)
    survey_nodes = json.loads(survey_nodes)["data"]["allSurveys"]["nodes"]
    contributor_info = pd.DataFrame()
    all_responses = {}

    for node in survey_nodes:
        if node["survey"] != "023":
            # if survey is not rsi
            print("Found survey", node["survey"], "which does not match rsi")
            continue

        formtypes = pd.DataFrame(node["idbrformtypesBySurvey"]["nodes"])
        for contributor in node["contributorsBySurvey"]["nodes"]:
            contributor_responses = {}
            if contributor["status"] not in ["Clear"]:
                # Assuming we want a blank row to signify a non-responder
                period = contributor["period"]
                contributor_responses[period] = [{}]

            else:
                # Cleared data so load in the response. We get one response
                # per question code hence the loop and checking whether
                # we've already seen the period.
                response_dict = contributor["responsesByReferenceAndPeriodAndSurvey"]
                for response in response_dict["nodes"]:
                    period = response["period"]
                    if period not in contributor_responses:
                        contributor_responses[period] = [response]

                    else:
                        contributor_responses[period].append(response)

            contributor_ref = contributor["reference"]
            if contributor_ref not in all_responses:
                contributor_info = pd.concat(
                    [
                        contributor_info,
                        pd.DataFrame(contributor)
                        .drop(
                            ["responsesByReferenceAndPeriodAndSurvey", "period"], axis=1
                        )
                        .reset_index(),
                    ]
                )
                all_responses[contributor_ref] = {}

            existing_responses = all_responses[contributor_ref]
            for period, responses in contributor_responses.items():
                if period not in existing_responses:
                    existing_responses[period] = responses

                else:
                    existing_responses[period] += responses

    print("Ingested rows:", sum(len(val) for val in all_responses.values()))

    questions = {"20", "21", "22", "23", "24", "25", "26", "27"}
    output_rows = []

    for ref, responses in all_responses.items():
        for period, response_values in responses.items():
            output_row = {
                "run_id": run_id,
                "reference": ref,
                "period": period
            }
            # The non-responders will not be iterated over but pandas will fill
            # in the columns as empty
            for response in filter(
                lambda r: r.get("questioncode") in questions, response_values
            ):
                question_name = "Q{}".format(response["questioncode"])
                try:
                    response_val = float(response["response"])

                except Exception:
                    response_val = None

                output_row[question_name] = response_val

                try:
                    adj_val = float(response["averageweeklyadjustedresponse"])

                except Exception:
                    adj_val = None

                output_row[f"average_weekly_q{response['questioncode']}"] = adj_val

            # Fill in the missing questions as otherwise pandas explodes
            for question in questions:
                question_name = f"Q{question}"
                if question_name not in output_row:
                    output_row[question_name] = None

                average_name = f"average_weekly_q{question}"
                if average_name not in output_row:
                    output_row[average_name] = None

            output_rows.append(output_row)

    output_rows_df = pd.DataFrame(output_rows)

    contributor_info = contributor_info.drop_duplicates()[
        [
            "reference",
            "referencename",
            "enterprisereference",
            "rusic",
            "frozensic",
            "frozenturnover",
            "region",
            "cellnumber",
            "employment",
            "formid",
        ]
    ]
    output = pd.merge(output_rows_df, contributor_info, how="left", on="reference")

    output = pd.merge(output, formtypes[["formid", "formtype"]], on="formid")
    output = output.rename(columns={"formtype": "instrument_id"})

    output = output[
        [
            "run_id",
            "reference",
            "period",
            "Q20",
            "average_weekly_q20",
            "Q21",
            "average_weekly_q21",
            "Q22",
            "average_weekly_q22",
            "Q23",
            "average_weekly_q23",
            "Q24",
            "average_weekly_q24",
            "Q25",
            "average_weekly_q25",
            "Q26",
            "average_weekly_q26",
            "Q27",
            "average_weekly_q27",
            "referencename",
            "enterprisereference",
            "rusic",
            "frozensic",
            "frozenturnover",
            "region",
            "cellnumber",
            "employment",
            "instrument_id",
        ]
    ]
    save_dataframe_to_csv(output, config["IngestedLocation"], "RSI/ingested/output")


def enrich(config):
    execution_context = {"Database": "spp_res_ath_business_surveys"}
    athena_query = """
    INSERT INTO spp_res_tab_rsi_ingestedstaged

    with organised_weights as
        (SELECT period,
                classification,
                cell_no,
                question_no,
                g_weight,
                a_weight
        FROM "spp_res_ath_business_surveys"."spp_res_tab_rsi_aglookup"
        WHERE question_no = '20' )

    SELECT a.run_id as run_id,
           a.reference as ruref,
           CAST(a.period as integer) as period,
           c.domain,
           CAST(a.cellnumber  as integer) as cell,
           CASE WHEN
                SUBSTR(a.cellnumber, -1) = '4' OR SUBSTR(a.cellnumber, -1) = '5'
           THEN '6'
           ELSE a.cellnumber END as impclass,
           CAST(a.frozenturnover as double) as frozen_turnover,
           CAST(a.rusic as integer) as rusic2007,
           'Y' as selected,
           CAST(c.threshold as integer) as score_threshold,
           a.instrument_id,
           666 as ref_period_start_date,
           666 as ref_period_end_date,
           666 as reported_start_date,
           666 as reported_end_date,
           a.average_weekly_q20,
           a.average_weekly_q21,
           a.average_weekly_q22,
           a.average_weekly_q23,
           a.average_weekly_q24,
           a.average_weekly_q25,
           a.average_weekly_q26,
           a.average_weekly_q27,
           666 as start_date,
           666 as end_date,
           CAST(b.a_weight as double) as design_weight,
           CAST(b.g_weight as double) as calibration_weight
    FROM spp_res_tab_rsi_ingested a,
         organised_weights b,
         spp_res_tab_rsi_domaingroupings c
    WHERE a.period=b.period
    AND b.classification = a.rusic
    AND b.cell_no = a.cellnumber
    AND a.cellnumber=c.cell
    """
    client = boto3.client("athena")
    result = do_query(
        client,
        athena_query,
        {"OutputLocation": config["OutputLocation"]},
        execution_context,
    )
    print(result)


def split_s3_path(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key


def emptyfolders(config):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(config["IngestedLocation"])
    bucket.objects.filter(Prefix="RSI/ingestedstaged/").delete()


config = json.loads(get_from_file())
emptyfolders(config)
snapshot_location = getResolvedOptions(sys.argv, ["config"])
config_str = base64.b64decode(snapshot_location["config"].encode("ascii")).decode(
    "ascii"
)
snapshot_location_config = json.loads(config_str)
snapshot_location_bucket, snapshot_location_key = split_s3_path(
    snapshot_location_config["snapshot_location"]
)
run_id = snapshot_location_config['pipeline']['run_id']

ingest(config, snapshot_location_bucket, snapshot_location_key, run_id)
enrich(config)
