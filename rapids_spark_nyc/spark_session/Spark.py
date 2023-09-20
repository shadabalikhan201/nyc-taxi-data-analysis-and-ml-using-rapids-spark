import os
import findspark
from pyspark.sql import SparkSession
from rapids_spark_nyc.utilities.SparkUtil import SparkUtil


class Spark:
    __session = None

    @staticmethod
    def __get_spark_config_jars(jars_directory: str) -> str:
        files = os.listdir(jars_directory)
        print(files)
        spark_config_jars = ''
        i = 0
        for file in files:
            if i == 0:
                spark_config_jars += jars_directory + '/' + file
                i += 1
            else:
                spark_config_jars += ',' + jars_directory + '/' + file
        return spark_config_jars

    @staticmethod
    def __init_spark_session(project_home: str):
        findspark.init()
        if Spark.__session is None:
            Spark.__session = (SparkSession.builder.appName("PySpark GPU HelloWorld").master("local")
                               .config("spark.jars",
                                       Spark.__get_spark_config_jars(project_home + '/resourcess/dependency_jars'))
                               .config("spark.executor.resource.gpu.discoveryScript",
                                       "resourcess/shell_scripts/getGpusResources.sh")
                               .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
                               .config("spark.rapids.sql.incompatibleOps.enabled", "true")
                               .config("spark.rapids.sql.enabled", "true")
                               .config("spark.rapids.gpu.resourceName", "GPU-9106a196-8414-4546-79fc-b6e893da9376")
                               .config("spark.rapids.memory.gpu.allocFraction", "0.65")
                               .config("spark.rapids.memory.gpu.maxAllocFraction", "0.9")
                               .config("spark.rapids.memory.gpu.minAllocFraction", "0")
                               .config("spark.rapids.memory.gpu.pool", "ASYNC")
                               .config("spark.dynamicAllocation.enabled", "false")
                               .config("spark.executor.memory", "4g")
                               .config("spark.executor.resource.gpu.amount", "1")
                               .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                               .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
                               .config("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")
                               .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                               .config("spark.rapids.sql.exec.CollectLimitExec", "true")
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
                               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,"
                                                               "org.apache.iceberg.spark.extensions"
                                                               ".IcebergSparkSessionExtensions")
                               .config("spark.sql.catalog.spark_catalog",
                                       "org.apache.spark.sql.delta.catalog.DeltaCatalog,"
                                       "org.apache.iceberg.spark.SparkSessionCatalog")
                               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                               .config("spark.kryo.registrator", "com.nvidia.spark.rapids.GpuKryoRegistrator")
                               .getOrCreate())

    @staticmethod
    @SparkUtil.returns_spark_session
    def get_spark_session(project_home: str) -> SparkSession:
        Spark.__init_spark_session(project_home)
        Spark.__session.sparkContext._jvm.java.lang.String("x")
        return Spark.__session

    @staticmethod
    def destroy_spark_session():
        Spark.__session.stop()
