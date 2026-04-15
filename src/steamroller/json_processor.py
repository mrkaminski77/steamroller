import re
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

def drill_and_flatten(df, json_path: str, delimiter="__", literal_dot_replacement="_"):
    raw_parts = re.split(r"(?<!\\)\.", json_path)
    parts = [p.replace(r"\.", ".") for p in raw_parts]
    
    # Pre-calculate the final prefix to identify child structs later
    target_prefix = delimiter.join([p.replace(".", literal_dot_replacement) for p in parts])
    
    current_col = parts.pop(0)

    # Phase 1: Drill down and flatten the exact path
    while parts or (current_col in df.columns and isinstance(df.schema[current_col].dataType, (StructType, ArrayType))):
        dtype = df.schema[current_col].dataType

        if isinstance(dtype, ArrayType):
            df = df.withColumn(current_col, F.explode_outer(current_col))
            
        elif isinstance(dtype, StructType):
            other_cols = [c for c in df.columns if c != current_col]
            expanded_cols = []
            
            for f in dtype.fields:
                safe_field_name = f.name.replace(".", literal_dot_replacement)
                expanded_cols.append(
                    F.col(f"`{current_col}`.`{f.name}`").alias(f"{current_col}{delimiter}{safe_field_name}") 
                )
                
            df = df.select(*other_cols, *expanded_cols)
            
            if parts:
                next_part = parts.pop(0).replace(".", literal_dot_replacement)
                current_col = f"{current_col}{delimiter}{next_part}"
            else:
                break 
                
        else:
            break 

    # Phase 2: Recursively flatten any child structs belonging to the target
    while True:
        child_structs = [
            f.name for f in df.schema.fields 
            if f.name.startswith(f"{target_prefix}{delimiter}") and isinstance(f.dataType, StructType)
        ]
        
        if not child_structs:
            break
            
        select_exprs = []
        for f in df.schema.fields:
            if f.name in child_structs:
                for sub_field in f.dataType.fields:
                    safe_field_name = sub_field.name.replace(".", literal_dot_replacement)
                    select_exprs.append(
                        F.col(f"`{f.name}`.`{sub_field.name}`").alias(f"{f.name}{delimiter}{safe_field_name}")
                    )
            else:
                select_exprs.append(F.col(f"`{f.name}`"))
                
        df = df.select(*select_exprs)

    return df
	
def prune_to_relationship(df, json_path: str, delimiter="__", literal_dot_replacement="_"):
    raw_parts = re.split(r"(?<!\\)\.", json_path)
    parts = [p.replace(r"\.", literal_dot_replacement) for p in raw_parts]
    target_prefix = delimiter.join(parts)
    
    cols_to_keep = []
    
    for f in df.schema.fields:
        if f.name.startswith(target_prefix):
            # 1. Keep target fields, but explicitly drop nested child arrays
            if not isinstance(f.dataType, ArrayType):
                cols_to_keep.append(f.name)
        elif not isinstance(f.dataType, (StructType, ArrayType)):
            # 2. Keep parent attributes (primitives) and drop sibling branches
            cols_to_keep.append(f.name)

    return df.select(*cols_to_keep)	
	
def add_metadata(df, json_path: str, run_id):
    return df.withColumn("_source_file", F.input_file_name()) \
             .withColumn("_load_timestamp", F.current_timestamp()) \
             .withColumn("_pipeline_run_id", F.lit(run_id)) \
             .withColumn("_target_grain", F.lit(json_path)) \
             .withColumn("_record_hash", F.xxhash64(*df.columns))	
			 
def add_surrogate_key(df, key_parts, key_name="row_wid", delimiter="__"):
    """
    Generates a deterministic surrogate key based on specified JSON paths.
    key_parts: List of dot-notation strings, e.g., ["customer.id", "order_id"]
    """
    # 1. Map dot-notation to our flattened column names
    # Note: Using the same logic as our drill function
    flattened_keys = [p.replace(".", delimiter) for p in key_parts]
    
    # 2. Handle potential nulls by coalescing to a dummy string 
    # to ensure the hash is stable and distinct
    hash_inputs = [F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("NA")) for c in flattened_keys]
    
    # 3. Generate the hash
    # xxhash64 is significantly faster than sha2 for large datasets
    return df.withColumn(key_name, F.xxhash64(*hash_inputs))			 