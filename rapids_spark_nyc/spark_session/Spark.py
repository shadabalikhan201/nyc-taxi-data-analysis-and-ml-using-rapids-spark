import os
import findspark
from delta import configure_spark_with_delta_pip
from loguru import logger
from pyspark.sql import SparkSession
from rapids_spark_nyc.utilities.SparkUtil import SparkUtil


class Spark:
    __session = None

    @staticmethod
    def __get_spark_config_jars(jars_directory: str) -> str:
        logger.info('start of Spark class __get_spark_config_jars() method')

        files = os.listdir(jars_directory)
        spark_config_jars = ''
        i = 0
        for file in files:
            if i == 0:
                spark_config_jars += jars_directory + '/' + file
                i += 1
            else:
                spark_config_jars += ',' + jars_directory + '/' + file

        logger.info('returning from Spark class __get_spark_config_jars() method')
        return spark_config_jars

    @staticmethod
    def __init_spark_session(project_home: str):
        logger.info('start of Spark class __init_spark_session() method')

        findspark.init()
        if Spark.__session is None:
            Spark.__session = configure_spark_with_delta_pip(
                (SparkSession.builder.appName("NYC Taxi Data Analysis and ML App").master("local")
                 .config("spark.jars",
                         Spark.__get_spark_config_jars(project_home + '/resources/dependency_jars'))
                 .config("spark.executor.resource.gpu.discoveryScript",
                         project_home + '/resources/shell_scripts/getGpusResources.sh')
                 .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
                 .config("spark.rapids.sql.incompatibleOps.enabled", "true")
                 .config("spark.rapids.sql.enabled", "true")
                 .config("spark.rapids.gpu.resourceName", "GPU-9106a196-8414-4546-79fc-b6e893da9376")
                 .config("spark.rapids.memory.gpu.allocFraction", "0.85")
                 .config("spark.rapids.memory.gpu.maxAllocFraction", "1.0")
                 .config("spark.rapids.memory.gpu.minAllocFraction", "0")
                 .config("spark.rapids.memory.gpu.pool", "ASYNC")
                 .config("spark.dynamicAllocation.enabled", "true")
                 .config("spark.executor.memory", "16g")
                 .config("spark.driver.memory", "16g")
                 .config("spark.executor.resource.gpu.amount", "1")
                 .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                 .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
                 .config("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")
                 .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                 .config("spark.rapids.sql.exec.CollectLimitExec", "true")
                .config('spark.driver.maxResultSize', '4g')
                 .config("spark.sql.inMemoryColumnarStorage.batchSize", "200000")
                 .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
                 .config("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
                 .config("spark.rapids.sql.format.csv.enabled", "true")
                 .config("spark.rapids.sql.format.csv.read.enabled", "true")
                 .config("spark.rapids.sql.csv.read.decimal.enabled", "true")
                 .config("spark.rapids.sql.csv.read.double.enabled", "true")
                 .config("spark.rapids.sql.csv.read.float.enabled", "true")
                 .config("spark.rapids.sql.format.parquet.enabled", "true")
                 .config("spark.rapids.sql.format.parquet.read.enabled", "true")
                 .config("spark.rapids.sql.format.parquet.reader.footer.type", "AUTO")
                 .config("spark.rapids.sql.format.parquet.reader.type", "AUTO")
                 .config("spark.rapids.sql.explain", "NONE")
                 .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                 #.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 #.config("spark.kryo.registrator", "com.nvidia.spark.rapids.GpuKryoRegistrator")
                 .config("spark.sql.warehouse.dir", project_home + "/resources/output_dir/warehouse")
                 )).enableHiveSupport().getOrCreate()

        logger.info('returning from Spark class __init_spark_session() method')

    @staticmethod
    # @SparkUtil.returns_spark_session
    def get_spark_session(project_home: str) -> SparkSession:
        logger.info('start of Spark class get_spark_session() method')

        Spark.__init_spark_session(project_home)
        Spark.__session.sparkContext._jvm.java.lang.String("x")

        logger.info('returning from Spark class get_spark_session() method')
        return Spark.__session

    @staticmethod
    def destroy_spark_session():
        logger.info('start of Spark class destroy_spark_session() method')
        Spark.__session.stop()
        logger.info('returning from Spark class destroy_spark_session() method')
