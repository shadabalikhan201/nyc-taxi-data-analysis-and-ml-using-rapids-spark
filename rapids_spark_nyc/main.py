import sys

from delta import DeltaTable
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