from funciones import *

from pyspark.sql import functions as F
import  pyspark.sql.column
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType,FloatType
from pyspark.sql.functions import col, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col



def test_NoValidParameterGetGroupByNameAndCareer(spark_session):
    #validar que no falle si se envian parametros no validos
    my_emptyResult = GetGroupByNameAndCareer('',spark_session)
    
    assert my_emptyResult.count() == 0
    print ("test_NoValidParameterGetGroupByNameAndCareer OK")


def test_GetGroupByNameAndCareer_NoValidCreditos(spark_session):
    # validad posible division por cero en la columna de creditos
    data = [(1,'test student 1', 'medicina',1,1,70,1,0,'medicina',0), 
                          (2,'test student 2', 'medicina',1,1,80,1,0,'medicina',0)]
    df = spark_session.createDataFrame(data,
                                              ['carnet', 'nombre','carrera','carnet_curso','cod_curso','nota','cod_curso',
                                              'creditos','carrera_curso','Notas_Creditos'])
    actual_ds = GetGroupByNameAndCareer(df,spark_session)
    
    assert actual_ds.count() == 2
    
    print ("test_GetGroupByNameAndCareer_emptyFrame OK")


def test_GetGroupByNameAndCareer_EmpyFrame(spark_session):
    # validar la funcion de agrupar, no debe fallar aunque se le mande un dataframe vacio
    
    schema = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('carnet_curso', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType()),
                        StructField('cod_curso', IntegerType()),
                        StructField('creditos', IntegerType()),
                        StructField('carrera_curso', StringType()),
                        StructField('Notas_Creditos', FloatType()),
                        StructField('Promedio', FloatType())])
        
    data = []
    df = spark_session.createDataFrame(data,schema)
    actual_ds = GetGroupByNameAndCareer(df,spark_session)
    
    assert actual_ds.count() == 0
    
    print ("test_GetGroupByNameAndCareer_EmpyFrame OK")




def test_GetGroupByNameAndCareer_ByNullValues(spark_session):
    # check wint two empty rows, expected row count =1
    data = [(1,'', '',1,1,70,1,0,'',0), 
                          (2,'', '',1,1,80,1,0,'',0)]
    df = spark_session.createDataFrame(data,
                                              ['carnet', 'nombre','carrera','carnet_curso','cod_curso','nota','cod_curso',
                                              'creditos','carrera_curso','Notas_Creditos'])
    actual_ds = GetGroupByNameAndCareer(df,spark_session)
    
    assert actual_ds.count() == 1
    
    print ("test_GetGroupByNameAndCareer_NoValidCreditos OK")


def test_GetGroupByNameAndCareer_NormalData(spark_session):
    # group normal debe retornar una sola fila
    data = [(1,'test student 1', 'medicina',1,1,70,1,0,'medicina',0), 
                          (1,'test student 1', 'medicina',1,1,80,1,0,'medicina',0),
			 (1,'test student 1', 'medicina',1,1,90,1,0,'medicina',0)]
    df = spark_session.createDataFrame(data,
                                              ['carnet', 'nombre','carrera','carnet_curso','cod_curso','nota','cod_curso',
                                              'creditos','carrera_curso','Notas_Creditos'])
    actual_ds = GetGroupByNameAndCareer(df,spark_session)
    
    assert actual_ds.count() == 1
    
    print ("test_GetGroupByNameAndCareer_NormalData OK")

