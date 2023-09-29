from typing import Optional, Union
from loguru import logger
from pyspark.sql import DataFrame, DataFrameWriter


class Writer:
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