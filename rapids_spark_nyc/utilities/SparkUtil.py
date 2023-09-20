from pyspark.sql import SparkSession, DataFrame


class SparkUtil:

    @staticmethod
    def returns_spark_session(func):
        def spark_session_wrapper(*args):
            result = func(*args)
            assert type(result) == SparkSession
            return result

        return spark_session_wrapper

    @staticmethod
    def returns_spark_dataframe(func):
        def spark_dataframe_wrapper(*args):
            result = func(*args)
            assert type(result) == DataFrame
            return result

        return spark_dataframe_wrapper
