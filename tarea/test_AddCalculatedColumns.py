from funciones import *

from pyspark.sql import functions as F
import  pyspark.sql.column
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType,FloatType
from pyspark.sql.functions import col, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

def test_Add_calculatedColums(spark_session):
    
    #validar que la columna se agregue correctamente y de valores correctos
    StudentGrades_data = [(1,'test student 1', 'medicina',1,1,70,1,4,'medicina'), 
                          (1,'test student 1', 'medicina',1,1,80,1,2,'medicina')]
    StudentGrade_ds = spark_session.createDataFrame(StudentGrades_data,
                                              ['carnet', 'nombre','carrera','carnet','cod_curso','nota','cod_curso',
                                              'creditos','carrera'])

    
    actual_ds = AddCalculatedColumns(StudentGrade_ds,spark_session)
    
    
    total = actual_ds.groupBy().agg(F.sum("Notas_Creditos")).collect()
    cred = total[0][0]
    
    assert  cred == 440
    print ("test_Add_calculatedColums OK")



def test_NoValidParameterAddCalculatedColum(spark_session):
    #valida que la funcion no falle si se manda cualquier parametro como dataframe
    my_emptyResult = AddCalculatedColumns('',spark_session)
    
    assert my_emptyResult.count() == 0
    print ("test_NoValidParameterAddCalculatedColum OK")


def test_Add_calculatedColumWithNullValues(spark_session):
    
    #validar que la funcion no falle ciando se le mandan valores nulos
    StudentGrades_data = [(1,'test student 1', 'medicina',1,1,70,1,'null','null'), 
                          (1,'test student 1', 'medicina',1,1,80,1,'null','null')]
    StudentGrade_ds = spark_session.createDataFrame(StudentGrades_data,
                                              ['carnet', 'nombre','carrera','carnet','cod_curso','nota','cod_curso',
                                              'creditos','carrera'])

    
    actual_ds = AddCalculatedColumns(StudentGrade_ds,spark_session)
    
    
    assert actual_ds.count() == 2
    
    print ("test_Add_calculatedColumWithNullValues OK")