"""
This script handles the end to end orchestration of the application for the 'nyc-taxi-rapids-spark' project.
The implementation uses the loguru library for logging various stages and exceptions in the application,

The main method is where the high-level orchestration of the application occurs. It initializes a Spark
session, uses a `Reader` class to read Parquet file data, creates a SQL schema, writes out the resultant
DataFrame in 'delta' format, and initializes an EDA dashboard. The Spark session is terminated at the end,
and any exceptions are logged and propagated.

Furthermore, the `__name__ == '__main__'` block ensures that the code is executed only when the script
is run directly, not when imported as a module. It runs the `main` method and logs the start, successful
completion, or failure of the application.

Functions
---------
main():
    Orchestrates the process involving the creation of a Spark session, data reading and writing,
    schema generation, and dashboard setup.
"""

import sys
from loguru import logger
from rapids_spark_nyc.dashboard.eda.EdaHomeDashboard import EdaDashboard
from rapids_spark_nyc.read.Reader import Reader
from rapids_spark_nyc.spark_session.Spark import Spark
from rapids_spark_nyc.write.Writer import Writer

logger.configure(
    handlers=[
        # dict(sink=sys.stderr, format="[{time}] {message}", backtrace=False, ),
        dict(sink=sys.stdout, format="[{time}] {message}", backtrace=True, ),
        dict(sink=sys.argv[1] + "/resources/log/file.log", enqueue=True, serialize=True, backtrace=False, ),
    ],
    levels=[dict(name="NEW", no=13, icon="¤", color="")],
    extra={"common_to_all": "default"},
    patcher=lambda record: record["extra"].update(some_value=42),
    activation=[("my_module.secret", False), ("another_library.module", False)],
)


def main():
    """
    The main method from where the execution of the project begins.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Raises
    ------
    Exception
        If an exception occurs during the execution of the method, it logs it and re-raises it.

    Notes
    -----
    The spark session is created in this method using the `get_spark_session` method of the Spark class. The relative paths of the data files are hard-coded in this method.
    """
    logger.info('start of main() method')

    project_home = sys.argv[1]
    spark_session = Spark.get_spark_session(project_home)

    try:
        df = Reader().read(spark_session, 'parquet', True, '/home/optimus_prime/PycharmProjects/nyc-taxi-rapids-spark/resources/input_files/yellow_tripdata_2023-01.parquet')

        spark_session.sql("CREATE SCHEMA IF NOT EXISTS yellow_tripdata")
        Writer().write(df, 'delta', 'yellow_tripdata.jan', 'overwrite')
        EdaDashboard().get_dashboard_home('Radips_spark_nyc', [['yellow_tripdata_jan', 'yellow_tripdata.jan']])
    except Exception as ex:
        logger.exception('Exiting from the main() method due to exception')
        raise ex

    finally:
        Spark.destroy_spark_session()
    logger.info('returning from main() method')


if __name__ == '__main__':
    logger.info('start of the {} application'.format('Rapids_Spark_Nyc'))
    try:
        main()
        logger.success('{} application ended successfully'.format('Rapids_Spark_Nyc'))
    except Exception as ex:
        logger.error('{} application ended with an exception'.format('Rapids_Spark_Nyc'))