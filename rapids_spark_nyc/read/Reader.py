import sys
from typing import Optional
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException
from rapids_spark_nyc.utilities.SparkUtil import SparkUtil


class Reader:

    def __init__(self):
        logger.info('Reader class instantiation')

    @SparkUtil.returns_spark_dataframe
    def read(self, spark_session: SparkSession, input_format: str, header: bool, input_dir: str):
        logger.info('start of Reader class read() method')
        df = None
        try:
            file_path = input_dir

            if input_format == 'parquet':
                reader = spark_session.read
                if header:
                    reader = reader.option("header", True)
                df = reader.parquet(file_path)

        except AnalysisException as ex:
            logger.exception(ex)
            raise AnalysisException

        logger.info('returning from Reader class read() method')
        return df

    @SparkUtil.returns_spark_dataframe
    def read_versioned(self, spark_session: SparkSession, input_format: str, header: bool, table_name: str,
                       version: Optional[str] = None, time_stamp: Optional[str] = None):

        logger.info('start of Reader class read_versioned() method')

        df = None
        try:
            if input_format == 'delta':
                reader = spark_session.read.format('delta')
                if header:
                    reader = reader.option("header", True)
                if version is not None:
                    reader = reader.option('versionAsOf', int(version))
                if time_stamp is not None:
                    reader = reader.option('timestampAsOf', time_stamp)
                df = reader.table(table_name)

        except AnalysisException as ex:
            logger.exception(ex)
            raise AnalysisException

        logger.info('returning from Reader class read_versioned() method')
        return df