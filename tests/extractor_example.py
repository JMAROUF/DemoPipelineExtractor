import os
from typing import Dict, Any, Optional, Union, List
import pyspark
from pyspark import SparkConf
from pyspark.sql import DataFrame as SDF
from pyspark.sql.session import SparkSession

class ExtractorExample:
    def __init__(self, options: Optional[Dict[str, Any]] = None, filter_on: Optional[str] = None,
                 columns: Optional[Dict[str, Any]] = None, drop_duplicate: bool = True):
        self.options = options
        self.filter_on = filter_on
        self.columns = columns
        self.schema_ddl = "nom STRING, prenom STRING"
        self.drop_duplicate = drop_duplicate
    def extract(self, uri: Union[str, List[str]], spark: SparkSession) -> SDF:

        if self.options is not None:
            dataframe = (spark.read.format("csv").options(**self.options)
            .schema(self.schema_ddl if self.schema_ddl is not None else None)
            .load(uri))
        else:
            dataframe = (spark.read.format("csv")
            .schema(self.schema_ddl if self.schema_ddl is not None else None)
            .load(uri))

        if self.columns is not None:
            if self.columns["type"] == "include":
                dataframe = dataframe.selectExpr(*self.columns["list"])
            else:
                dataframe = dataframe.drop(*self.columns["list"])
        if self.filter_on is not None:
            dataframe = dataframe.filter(dataframe["nom"].startswith("D"))
        if self.drop_duplicate:
            dataframe = dataframe.dropDuplicates()
        return dataframe

if __name__ == "__main__":
    spark=None
    try:
     spark = (SparkSession.builder
                     .appName("extractorExample")
              .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
              .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.profile.ProfileCredentialsProvider")
              .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
              .config("spark.hadoop.fs.s3a.aws.profile", os.getenv("AWS_PROFILE"))
              .getOrCreate())
     spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
     print(f"Error {e}")


    if spark:
        #Chemin du fichier CSV
        #csv_file_path = "test_extract.csv"

        # df = (spark.read.format("csv")
        #                .option("header", "true")
        #                .option("inferSchema", "true")
        #                .load(csv_file_path))


        extractor_config = {
         "options": {
             "header": "true",
             "inferSchema": "true"
         },
         "columns": {
             "type": "include",
             "list": ["nom", "prenom"]
         },
         "filter_on": "true",
         "drop_duplicate": True
        }
        input = "s3a://aws-sto-s3s-cdn-u-apps/test_extract.csv"

    #    input = "test_extract.csv"
        extractor = ExtractorExample(**extractor_config)
        df = extractor.extract(input, spark)
        df.show()
    else :
        print("failed to create session")