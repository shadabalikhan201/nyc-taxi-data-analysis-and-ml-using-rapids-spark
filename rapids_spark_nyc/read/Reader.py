from typing import Optional
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.errors.exceptions.captured import AnalysisException


class Reader:
    """
    The Reader class provides functionality to read data from various sources using Spark.

    This class provides methods to read data both from non-versioned data sources like parquet files and from versioned Delta lake.
    In case of exceptions during the read operation, these are caught, logged, and re-raised.

    Methods
    -------
    read(spark_session: SparkSession, input_format: str, header: bool, input_dir: str) -> DataFrame:
        Reads a data file of a specified format from a provided directory and returns it as a DataFrame.

    read_versioned(spark_session: SparkSession, input_format: str, header: bool, table_name: str,
                version: Optional[str] = None, time_stamp: Optional[str] = None) -> DataFrame:
        Reads a versioned Delta format data file and returns it as a DataFrame. Version or timestamp can be provided for specific reads, else the latest version is read.

    Note
    ----
    The read_versioned method currently only supports the 'delta' format. Iceberg and Hudi to be implemented.
    """
    def __init__(self):
        logger.info('Reader class instantiation')

    def read(self, spark_session: SparkSession, input_format: str, header: bool, input_dir: str) -> DataFrame:
        """
        Reads a data source of a specified format from a provided location.

        Currently, it only supports Parquet format, other non-versioned data sources to be implemented later.

        Parameters
        ----------
        spark_session : SparkSession
            The Spark session object to use for reading.

        input_format : str
            The format of the input file to read. Currently, the only accepted format is 'parquet'.

        header : bool
            A flag indicating whether the file has a header. If True, the header row will be used as the column names.

        input_dir : str
            The directory of the input file to read.

        Returns
        -------
        DataFrame
            A DataFrame created from the input file.

        Raises
        ------
        AnalysisException
            If the read operation encounters any problems, such as the file or directory not existing.

        Notes
        -----
        In case of exceptions during the read operation, they are logged and re-raised.
        """
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

    def read_versioned(self, spark_session: SparkSession, input_format: str, header: bool, table_name: str,
                       version: Optional[str] = None, time_stamp: Optional[str] = None) -> DataFrame:
        """
        Reads a versioned data source and returns it as a DataFrame.

        This method reads a versioned Delta format data file from the specified table source, taking headers
        into account if required. Version or timestamp can be provided for specific reads.

        Parameters
        ----------
        spark_session : SparkSession
            The Spark session object to use for reading.

        input_format : str
            The format of the input file to read. Currently, the only accepted value is 'delta', other versioned data sources like Iceberg and Hudi to be implemented later.

        header : bool
            A flag indicating whether the file has a header. If True, the header row will be used as the column names.

        table_name : str
            The name of the table to read from.

        version : str, optional
            The version of the data to read. If not specified, the latest version is read.

        time_stamp: str, optional
            The timestamp of the data to read. If not specified, the data at the latest timestamp is read.

        Returns
        -------
        DataFrame
            A DataFrame created from the given versioned data source.

        Raises
        ------
        AnalysisException
            If the read operation encounters any problems, such as the table not existing.

        Notes
        -----
        In case of exceptions during the read operation, they are logged and re-raised.
        """
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
