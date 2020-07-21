from funciones import *

from pyspark.sql import functions as F
import  pyspark.sql.column
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType,FloatType
from pyspark.sql.functions import col, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


def test_NoValidPathGet_GradesDF(spark_session):
        #validar q retorne un dataframe vacio si se manda un path no existente
    my_emptyResult = Get_GradesDF(spark_session,'fakepath')
    
    my_emptyResult.show()
    
    assert my_emptyResult.count() == 0
    print ("test_NoValidPathGet_GradesDF OK")