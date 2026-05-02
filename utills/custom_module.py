from typing import List 
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, desc, current_timestamp
from pyspark.sql.functions import row_number
from pyspark.sql import Window
from pyspark.sql.functions import split
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

class transforms:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def dedup(self, df: DataFrame, dedup_cols: List, cdc: str):
        df = df.withColumn("dedup_keys", concat(*dedup_cols))
        df = df.withColumn("dedupCounts", 
            row_number().over(
                Window.partitionBy("dedup_keys")
                .orderBy(desc(cdc))
            ))
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop("dedupCounts", "dedup_keys")
        return df
    
    def process_time_stamp(self, df):
        df = df.withColumn("process_time", current_timestamp())
        return df

    def upsert(self, df, key_cols, table, cdc):
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
    
        spark = df.sparkSession
        dlt_obj = DeltaTable.forName(spark, f"first_project_catalogy.silver_schema.{table}")
    
        dlt_obj.alias("trg").merge(df.alias("src"), merge_condition) \
          .whenMatchedUpdateAll(
            condition=f"src.{cdc} >= trg.{cdc}"
           ) \
           .whenNotMatchedInsertAll() \
           .execute()
    
        return 1