from rapids_spark_nyc.exception.RapidsSparkException import NomeException


class RapidsSparkReadException(NomeException):
    def __init__(self, message:str):
        self.message = message