import os
import findspark
from delta import configure_spark_with_delta_pip
from loguru import logger
from pyspark.sql import SparkSession


class Spark:
    """
    Spark class manages the singleton instance of the SparkSession and is designed to handle all SparkSession specific operations for the application.

    This class is responsible for constructing, providing, and destructing the Spark session.
    It uses configurations to initialize the SparkSession for accelerated data processing on Nvidia's RTX gpus.

    Attributes
    ----------
    __session : SparkSession
        The Spark session object.

    Methods
    -------
    __generate_jar_paths(jars_directory: str) -> str
        Constructs a comma-separated string of paths to all jar files in a given directory.

    __init_spark_session(project_home: str)
        Initializes the __session attribute with a configured SparkSession, if it isn't already.

    get_spark_session(project_home: str) -> SparkSession
        Returns the SparkSession object. If it does not exist, this method will initialize it.

    destroy_spark_session()
        Stops the running SparkSession, which triggers the freeing of its resources.
    """
    __session = None

    @staticmethod
    def __generate_jar_paths(jars_directory: str) -> str:
        """
        This static and private method constructs a comma-separated string of paths to all extra dependency jar files in a given directory.

        Specifically, this method logs the start of its execution, then lists all files in the provided directory.
        It creates an empty string variable 'spark_config_jars', then iterates over the file list. For every file,
        if it is the first file, its path is directly appended to the 'spark_config_jars'. Else, its path is appended
        with a comma in front of ensure separation.
        Finally, the end of the method execution is logged and the 'spark_config_jars' string is returned.

        Parameters
        ----------
        jars_directory : str
            The directory containing the jar files.

        Returns
        -------
        str
            A comma separated string of the paths to all jar files in the given directory.
        """
        logger.info('start of Spark class generate_jar_paths() method')
        try:
            spark_config_jars = ''
            if os.path.isdir(jars_directory):
                files = os.listdir(jars_directory)
                logger.info('Generating jar paths from the directory {}'.format(jars_directory))
                jars = [os.path.join(jars_directory, file) for file in files if file.endswith('.jar')]
                spark_config_jars = ','.join(jars)
            logger.info('returning from Spark class generate_jar_paths() method')
            return spark_config_jars
        except OSError as e:
            logger.exception('Error occurred while listing files in directory: {}, details: {}'.format(jars_directory, str(e)))
            raise e

    @staticmethod
    def __init_spark_session(project_home: str):
        """
        This static and private method constructs a spark session with a series of configured settings.

        It first checks if the static and private variable `__session` is already initialized. If not,
        it constructs the `SparkSession` with a series of configurations specified for the application.

        Parameters
        ----------
        project_home : str
            The path to the home directory of the project.

        Returns
        -------
        None
        """
        logger.info('start of Spark class __init_spark_session() method')

        findspark.init()
        if Spark.__session is None:
            Spark.__session = configure_spark_with_delta_pip(
                (SparkSession.builder.appName("NYC Taxi Data Analysis and ML App").master("local[*]")
                 .config("spark.jars",
                         Spark.__generate_jar_paths(project_home + '/resources/dependency_jars'))
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
                 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 .config("spark.kryo.registrator", "com.nvidia.spark.rapids.GpuKryoRegistrator")
                 .config("spark.sql.warehouse.dir", project_home + "/resources/output_dir/warehouse")
                 )).enableHiveSupport().getOrCreate()

        logger.info('returning from Spark class __init_spark_session() method')

    @staticmethod
    def get_spark_session(project_home: str) -> SparkSession:
        """
        This static and public method initializes a SparkSession using a specific project's home directory and returns the session.

        More specifically, it first logs the start of the method execution, calls the private `__init_spark_session`
        method to initialize the spark session with the given project home directory. Then, it constructs a Java
        string from the Spark context of the session and logs the end of the method execution before returning the
        initialized session object.

        Parameters
        ----------
        project_home : str
            The path to the project's home directory.

        Returns
        -------
        SparkSession
            The initialized SparkSession object.
        """

        logger.info('start of Spark class get_spark_session() method')

        Spark.__init_spark_session(project_home)
        Spark.__session.sparkContext._jvm.java.lang.String("x")

        logger.info('returning from Spark class get_spark_session() method')
        return Spark.__session

    @staticmethod
    def destroy_spark_session():
        """
        This static and public method stops the Spark session that was acquired or constructed earlier.

        It is necessary to call this method at the end of the processing in order to stop the Spark session and free up the resources.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        logger.info('start of Spark class destroy_spark_session() method')
        Spark.__session.stop()
        logger.info('returning from Spark class destroy_spark_session() method')
