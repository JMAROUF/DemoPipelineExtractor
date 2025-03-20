import logging
from typing import List

import delta.tables as DT
from pyspark import StorageLevel
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame

from src.transform.datavault.base import DeltaVaultTableCreator

class DeltaVaultTable(DeltaVaultTableCreator):

    def __init__(self, table_name: str,
                 location: str,
                 tech_id: str,
                 columns: List[str] = None,
                 drop_on_conflict: bool = False,
                 historized_mode: bool = True,
                 repartition_number: int = 2,
                 database_name: str = "corp_cdn_tru",
                 mode=None):

        super().__init__(table_name=table_name, location=location, columns=columns, drop_on_conflict=drop_on_conflict,
                         database_name=database_name)
        self.table_name = table_name
        self.location = location
        self.columns = columns
        self.tech_id = tech_id
        self.drop_on_conflict = drop_on_conflict
        self.historized_mode = historized_mode
        self.repartition_number = repartition_number
        self.database_name = database_name
        self.mode = mode

    def transform(self, dataframe, spark):

        self.log.info("Delta Vault Table: input dataframe...")

        self.columns = dataframe.columns if self.columns is None else self.columns
        # select useful cols
        dataframe = self.select_columns_dataframe(dataframe=dataframe, columns=self.columns)
        # generate the hash

        dataframe = self.hash_dataframe_row(dataframe=dataframe, columns=self.columns)
        if self.mode == "upsert":
            final_df = self.not_archives_lines(dataframe=dataframe)
            self.update_delta_table(mode=self.mode, dataframe=final_df, spark=spark, tech_id=self.tech_id)
        else:
            if self.historized_mode:
                final_df = self.archive_lines(dataframe=dataframe, tech_id=self.tech_id, spark=spark,
                                              repartition_number=self.repartition_number)
                final_df = final_df.dropDuplicates()
            else:
                final_df = self.not_archives_lines(dataframe=dataframe)
                self.mode = "overwrite"
                final_df = final_df.dropDuplicates()
            self.update_delta_table(mode=self.mode, dataframe=final_df, spark=spark)

    def archive_lines(self, dataframe, tech_id, spark, repartition_number=2):
        self.log.info("Already having data")
        if DT.DeltaTable.isDeltaTable(spark, self.location):
            old_dataframe = spark.read.format("delta").load(self.location)
            old_dataframe.repartition(repartition_number)
            dataframe.repartition(repartition_number)

            old_dataframe.persist(StorageLevel.MEMORY_ONLY)
            dataframe.persist(StorageLevel.MEMORY_ONLY)

            lines_to_disable = old_dataframe.join(dataframe, old_dataframe.diff_hash == dataframe.diff_hash,
                                                  "leftanti").withColumn("is_active", F.lit(False))

            # in case line is deleted and after add

            lines_to_keep = self.set_lddts(old_dataframe.join(dataframe, old_dataframe.diff_hash == dataframe.diff_hash,
                                                              "leftsemi").withColumn("is_active", F.lit(True)))

            new_lines = dataframe.join(old_dataframe, old_dataframe.diff_hash == dataframe.diff_hash,
                                       "leftanti").withColumn("is_active", F.lit(True))

            new_lines = self.set_lddts(new_lines)

            old_dataframe.unpersist()

            final_df = reduce(DataFrame.unionAll,
                              [lines_to_disable, lines_to_keep, new_lines])
        else:
            self.log.info("First time to be run")
            new_lines_df = dataframe.withColumn("is_active", F.lit(True))
            logging.info("adding lddts....")
            final_df = self.set_lddts(new_lines_df)

        return final_df

    def not_archives_lines(self, dataframe):
        return self.set_lddts(dataframe.withColumn("is_active", F.lit(True)))

    def select_columns_dataframe(self, dataframe, columns):
        return dataframe.select(*columns)

    def hash_dataframe_row(self, dataframe, columns):
        # Example hash function, replace with actual implementation
        return dataframe.withColumn("diff_hash", F.sha2(F.concat_ws("||", *columns), 256))

    def set_lddts(self, dataframe):
        # Example implementation, replace with actual logic
        return dataframe.withColumn("lddts", F.current_timestamp())

    def update_delta_table(self, mode, dataframe, spark, tech_id=None):
        if mode == "overwrite":
            dataframe.write.format("delta").mode("overwrite").save(self.location)
        elif mode == "append":
            dataframe.write.format("delta").mode("append").save(self.location)
        elif mode == "upsert":
            delta_table = DT.DeltaTable.forPath(spark, self.location)
            delta_table.alias("tgt").merge(
                dataframe.alias("src"),
                "tgt.diff_hash = src.diff_hash"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()




from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DeltaVaultTableExample") \
    .getOrCreate()

# Define sample data
data = [
    Row(CustomerID="C001", Name="John Doe", Email="john.doe@example.com"),
    Row(CustomerID="C002", Name="Jane Smith", Email="jane.smith@example.com")
]

dataframe = spark.createDataFrame(data)

# Initialize DeltaVaultTable
table_name = "Hub_Customers"
location = "/path/to/delta/table"
tech_id = "customer_tech_id"
columns = ["CustomerID", "Name", "Email"]

delta_vault_table = DeltaVaultTable(
    table_name=table_name,
    location=location,
    tech_id=tech_id,
    columns=columns,
    drop_on_conflict=False,
    historized_mode=True,
    repartition_number=2,
    database_name="corp_cdn_tru",
    mode="overwrite"
)

# Transform and load data
delta_vault_table.transform(dataframe, spark)