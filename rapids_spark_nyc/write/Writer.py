from typing import Optional, Union
from loguru import logger
from pyspark.sql import DataFrame, DataFrameWriter


class Writer:
    """
    Class for writing DataFrame to different output formats.

    Attributes
    ----------
    None

    Methods
    -------
    __init__() :
        Initialize the Writer class.

    write(df : DataFrame, output_format : str, output_path : str, mode : Optional[str], partition_by : Union[str, list[str], None], compression : Optional[str], df_writer_options : Optional[dict])
        Write the DataFrame to the specified output format.

    write_parquet(df_writer : DataFrameWriter, output_path : str, mode : Optional[str], partition_by : Union[str, list[str], None], compression : Optional[str])
        Write the DataFrame to Parquet format.

    create_delta_table(df_writer : DataFrameWriter, output_path : str, mode : Optional[str], df_writer_options : Optional[dict])
        Create a Delta table from the DataFrame.

    """
    def __init__(self):
        logger.info('Writer class instantiation')

    def write(self, df: DataFrame, output_format: str, output_path: str, mode: Optional[str] = None,
              partition_by: Union[str, list[str], None] = None, compression: Optional[str] = None,
              df_writer_options: Optional[dict] = None):

        logger.info('start of Writer class write() method')

        df_writer = df.write
        if output_format == "parquet":
            self.write_parquet(df_writer=df_writer, output_path=output_path, mode=mode, partition_by=partition_by,
                               compression=compression)

        elif output_format == "delta":
            self.create_delta_table(df_writer=df_writer, output_path=output_path, mode=mode,
                                    df_writer_options=df_writer_options)

        logger.info('returning from Writer class write() method')

    def write_parquet(self, df_writer: DataFrameWriter, output_path: str, mode: Optional[str] = None,
                      partition_by: Union[str, list[str], None] = None, compression: Optional[str] = None):
        """
        Write the DataFrame to Parquet format.

        This method will attempt to write the DataFrame to a Parquet file format
        with the option to set the mode, partitioning and compression for the file.

        Parameters
        ----------
        df_writer : DataFrameWriter
            The writer object from a PySpark DataFrame.

        output_path : str
            The directory path for saving the file.

        mode : str, optional
            Specifies the saving mode. Default is None.

        partition_by : str or list, optional
            Specifies columns to partition the DataFrame. Default is None.

        compression : str, optional
            Specifies compression type: 'none', 'uncompressed', 'snappy',
            'gzip', 'lzo', 'brotli', 'lz4', or 'zstd'. Default is None.

        Returns
        -------
        None

        Raises
        ------
        Exception
            If it fails to create the Delta table, it will raise an Exception.
        """
        logger.info('start of Writer class write_parquet() method')

        if mode is not None:
            df_writer = df_writer.mode(mode)
        if partition_by is not None:
            df_writer = df_writer.partitionBy(partition_by)
        if compression is not None and compression in ['none', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4',
                                                       'zstd']:
            df_writer = df_writer.option("compression", compression)
        try:
            df_writer.parquet(output_path)
        except Exception as ex:
            logger.exception(ex)
            raise ex
        logger.info('returning from Writer class write_parquet() method')

    def create_delta_table(self, df_writer: DataFrameWriter, output_path: str, mode: Optional[str] = None,
                           df_writer_options: Optional[dict] = None):
        """
        Create a Delta table from the DataFrame.

        This method will attempt to write the DataFrame into a Delta table that is
        saved in the specified Spark Warehouse dir with an optional mode and additional options.

        Parameters
        ----------
        df_writer : DataFrameWriter
            The writer object from a PySpark DataFrame.

        output_path : str
            The directory path for saving the file.

        mode : str, optional
            Specifies the saving mode. If 'overwrite', the 'overwriteSchema' option is set to True. Default is None.

        df_writer_options : dict, optional
            Additional options to be passed to 'df_writer.option()'. Default is None.

        Returns
        -------
        None

        Raises
        ------
        Exception
            If it fails to create the Delta table, it will raise an Exception.
        """
        logger.info('start of Writer class create_delta_table() method')

        if mode is not None:
            df_writer = df_writer.mode(mode)
        if mode == 'overwrite':
            df_writer.option("overwriteSchema", "true")
        if df_writer_options is not None:
            option_keys = df_writer_options.keys()
            for key in option_keys:
                df_writer.option(key, df_writer_options.get(key))
        try:
            df_writer.saveAsTable(output_path)
        except Exception as ex:
            logger.exception(ex)
            raise ex
        logger.info('returning from Writer class create_delta_table() method')