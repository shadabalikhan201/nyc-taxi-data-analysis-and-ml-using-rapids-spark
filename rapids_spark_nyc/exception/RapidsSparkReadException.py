from typing import Optional, Dict
from pyspark.errors.exceptions.captured import AnalysisException


class RapidsSparkReadException(AnalysisException):
    def __init__(self, message: Optional[str], error_class: Optional[str]=None, message_parameters: Optional[Dict[str, str]]=None):
        super().__init__(message)
        self.message = message
        #self.__str__()

    def __str__(self):
        return self.message + "__str__"

    def __repr__(self):
        return self.message + "__repr__"
