from pyspark.sql import SparkSession
from rapids_spark_nyc.exception.RapidsSparkException import NomeException
from rapids_spark_nyc.exception.RapidsSparkReadException import NomeReadException
from rapids_spark_nyc.utilities.SparkUtil import SparkUtil


class File_Reader:

    @SparkUtil.returns_spark_dataframe
    def read(self, spark_session: SparkSession, input_format: str, header: bool, input_dir: str):
        df = None
        try:
            file_path = input_dir

            if input_format == 'parquet':
                reader = spark_session.read
                if header:
                    reader = reader.option("header", True)
                df = reader.parquet(file_path)

        except NomeReadException:
            raise (NomeException("RapidsSparkException, Failed in reading input files to dataframe"))
        return df

    @SparkUtil.returns_spark_dataframe
    def read_versioned(self, spark_session: SparkSession, input_format: str, version: int, time_stamp: str,
                       header: bool, input_dir: str):
        df = None
        try:
            file_path = input_dir
            if input_format == 'delta':
                reader = spark_session.read.format('delta')
                if header:
                    reader = reader.option("header", True)
                if version != -1:
                    reader = reader.option('versionAsOf', version)
                if time_stamp != "-1":
                    reader = reader.option('timestampAsOf', time_stamp)
                df = reader.table(file_path)

        except NomeReadException:
            raise (NomeException("RapidsSparkException, Failed in reading input files to dataframe"))
        return df
