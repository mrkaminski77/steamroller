import sys
import argparse

from steamroller import ingest_landing_to_bronze
import json
import base64
from pyspark.sql import SparkSession

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest raw JSON blobs into a Bronze Delta table.")
    parser.add_argument("--blob_addresses", type=str, required=True, help="base64-encoded JSON array of blob addresses to ingest")
    parser.add_argument("--source_metadata", type=str, required=True, help="base64-encoded JSON object with source metadata")
    parser.add_argument("--dq_audit_path", type=str, required=True, help="Path to write data quality audit logs")
    parser.add_argument("--bronze_path", type=str, required=True, help="Path to write ingested Bronze Delta table")
    parser.add_argument("--batch_id", type=str, required=True, help="Unique identifier for this ingestion batch")
    parser.add_argument("--schema", type=str, default=None, help="Optional path to JSON schema or base64-encoded JSON schema string")
    return parser.parse_args()

def base64_decode_json(encoded_str):
    decoded_bytes = base64.b64decode(encoded_str)
    decoded_str = decoded_bytes.decode('utf-8')
    return json.loads(decoded_str)

def main():
    args = parse_args()

    blob_addresses = base64_decode_json(args.blob_addresses)
    source_metadata = base64_decode_json(args.source_metadata)

    # If schema is provided as a base64-encoded JSON string, decode it. Otherwise, pass it as a path.
    if args.schema:
        try:
            schema = base64_decode_json(args.schema)
        except Exception:
            schema = args.schema  # Assume it's a path if decoding fails
    else:
        schema = None

    ingest_landing_to_bronze(
        blob_addresses=blob_addresses,
        source_metadata=source_metadata,
        dq_audit_path=args.dq_audit_path,
        bronze_path=args.bronze_path,
        batch_id=args.batch_id,
        schema=schema,
    )


if __name__ == "__main__":
    main()
