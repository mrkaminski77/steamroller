import json
import datetime
from pyspark.sql import SparkSession, functions as F

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    MapType, LongType, IntegerType, BooleanType,
)

audit_schema = StructType([
    StructField("source_system", StringType(), True),
    StructField("source_entity", StringType(), True),
    StructField("pipeline_run_id", StringType(), True),
    StructField("query_start_time", TimestampType(), True),
    StructField("query_end_time", TimestampType(), True),
    StructField("query_params", MapType(StringType(), StringType()), True),
    StructField("spark_ui_url", StringType(), True),
    StructField("record_count", LongType(), True),
    StructField("file_count", IntegerType(), True),
    StructField("bad_record_count", LongType(), True),
    StructField("column_count", IntegerType(), True),
    StructField("schema_snapshot", StringType(), True),
    StructField("schema_enforced", BooleanType(), True),
    StructField("schema_drift", StringType(), True),
    StructField("duration_seconds", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("processed_at", TimestampType(), True),
])


def ingest_landing_to_bronze(
    blob_addresses: list,
    source_metadata: dict,
    dq_audit_path: str,
    bronze_path: str,
    schema: StructType = None,
):
    """
    Ingests raw JSON blobs into a Bronze Delta table and logs metadata to an audit table.

    source_metadata should include:
        - source_system: Name of the source system (e.g., "Salesforce", "AzureSQL")
        - source_entity: The specific entity or table being ingested (e.g., "Accounts", "Transactions")
        - query_window: Optional dict with 'start' and 'end' timestamps for the data being ingested
        - query_params: Optional dict of any parameters used in the data extraction (e.g., filters, batch identifiers)

    schema: Optional PySpark StructType. If provided, the read skips inference and the audit
            record will include any columns present in the schema but missing from the data
            (schema_drift), and any unexpected columns present in the data.
    """
    import mssparkutils
    spark = SparkSession.getActiveSession()
    ingestion_start = datetime.datetime.now()
    status = "success"
    error_message = None
    record_count = 0
    bad_record_count = 0
    column_count = 0
    schema_snapshot = None
    schema_enforced = schema is not None
    schema_drift = None
    pipeline_run_id = None
    spark_ui_url = None

    # --- Extract job context ---
    context = json.loads(mssparkutils.env.getJobContext())
    pipeline_run_id = context.get("pipelineRunId")
    workspace_name = context.get("workspaceName")
    spark_pool_name = context.get("sparkPoolName")
    job_id = context.get("jobId")
    app_id = spark.sparkContext.applicationId

    spark_ui_url = (
        f"https://web.azuresynapse.net/sparkui/{workspace_name}/"
        f"sparkpools/{spark_pool_name}/"
        f"sessions/{job_id}/"
        f"applications/{app_id}"
    )

    if not blob_addresses:
        raise ValueError("blob_addresses must contain at least one path.")

    try:
        # --- 1. Read raw JSON using PERMISSIVE mode to capture malformed records ---
        corrupt_col = "_corrupt_record"
        reader = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", corrupt_col)

        if schema is not None:
            # Append corrupt_record field to the caller-supplied schema so PERMISSIVE mode works
            enforced_schema = StructType(schema.fields + [StructField(corrupt_col, StringType(), True)])
            reader = reader.schema(enforced_schema)

        df_raw = reader.json(blob_addresses).cache()

        bad_record_count = df_raw.filter(F.col(corrupt_col).isNotNull()).count()
        df_clean = df_raw.drop(corrupt_col)

        record_count = df_clean.count()
        actual_cols = set(f.name for f in df_clean.schema.fields)
        column_count = len(actual_cols)
        schema_snapshot = json.dumps(sorted(actual_cols))

        # --- Drift detection (only when a schema was provided) ---
        if schema is not None:
            expected_cols = set(f.name for f in schema.fields)
            missing = expected_cols - actual_cols
            unexpected = actual_cols - expected_cols
            drift_parts = []
            if missing:
                drift_parts.append(f"missing: {sorted(missing)}")
            if unexpected:
                drift_parts.append(f"unexpected: {sorted(unexpected)}")
            schema_drift = "; ".join(drift_parts) if drift_parts else None

        # --- 2. Append ingestion metadata ---
        df_bronze = (
            df_clean
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_system", F.lit(source_metadata.get("source_system")))
            .withColumn("_source_entity", F.lit(source_metadata.get("source_entity")))
            .withColumn("_input_file_name", F.input_file_name())
            .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
        )

        # --- 3. Write to Bronze Delta table ---
        # Only enable mergeSchema when no schema was enforced or drift was detected,
        # to avoid the per-commit metadata overhead when the schema is known-stable.
        merge_schema = schema is None or schema_drift is not None
        df_bronze.write.format("delta") \
                 .mode("append") \
                 .option("mergeSchema", str(merge_schema).lower()) \
                 .save(bronze_path)

    except Exception as e:
        status = "failed"
        error_message = str(e)
        raise

    finally:
        ingestion_end = datetime.datetime.now()
        duration_seconds = (ingestion_end - ingestion_start).total_seconds()

        query_window = source_metadata.get("query_window") or {}
        audit_data = [{
            "source_system": source_metadata.get("source_system"),
            "source_entity": source_metadata.get("source_entity"),
            "pipeline_run_id": pipeline_run_id,
            "query_start_time": query_window.get("start"),
            "query_end_time": query_window.get("end"),
            "query_params": {k: str(v) for k, v in (source_metadata.get("query_params") or {}).items()},
            "spark_ui_url": spark_ui_url,
            "record_count": record_count,
            "file_count": len(blob_addresses),
            "bad_record_count": bad_record_count,
            "column_count": column_count,
            "schema_snapshot": schema_snapshot,
            "schema_enforced": schema_enforced,
            "schema_drift": schema_drift,
            "duration_seconds": str(round(duration_seconds, 3)),
            "status": status,
            "error_message": error_message,
            "processed_at": ingestion_end,
        }]

        try:
            audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
            audit_df.write.format("delta") \
                    .mode("append") \
                    .save(dq_audit_path)
        except Exception as audit_exc:
            import warnings
            warnings.warn(f"Audit write failed: {audit_exc}")

    return f"Ingested {record_count} records ({bad_record_count} bad) in {duration_seconds:.1f}s."