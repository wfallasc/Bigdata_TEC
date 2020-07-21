from funciones import *

from pyspark.sql import functions as F
import  pyspark.sql.column
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType,FloatType
from pyspark.sql.functions import col, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


def test_NoValidParameterGetTopByCareer(spark_session):
    #Validar parametros no validos en la funcion de obtener las notas por carrera
    my_emptyResult = GetTopByCareer('',spark_session)
    
    my_emptyResult.show()
    
    assert my_emptyResult.count() == 0
    print ("test_NoValidParameterGetTopByCareer OK")

def test_EmptyDFGetTopByCareer(spark_session):
    #obtener top de un dataframe vacio
    schema = StructType([StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('Notas_Creditos', FloatType()),
                         StructField('creditos', FloatType()),
                         StructField('Promedio', FloatType())])
    data = []
    df = spark_session.createDataFrame(data,schema)
    actual_ds = GetTopByCareer(df,spark_session)
    
    assert actual_ds.count() == 0
    
    print ("test_EmptyDFGetTopByCareer OK")


def test_NormalDataGetTopByCareer(spark_session):
    #Validar que los calculos sean correctos
    schema = StructType([StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('Notas_Creditos', FloatType()),
                         StructField('creditos', IntegerType()),
                         StructField('Promedio', FloatType())])
    
    schemaResult = StructType([StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('Notas_Creditos', FloatType()),
                         StructField('creditos', IntegerType()),
                         StructField('Promedio', FloatType()),
                         StructField('Rank', IntegerType()),])
    
    data = [(  "Est Medicina1" ,"Medicina", 1544.0, 22 ,70.18), 
                ( "Est. Fisica1", "Fisica" , 580.0,7,82.86),
                ( "Est. Fisica2", "Fisica" , 580.0,7,60.86),
           ("Est Medicina2","Medicina",820.0, 10,   40.0),
           ("Est Medicina3","Medicina",795.0, 11,72.27),
           ("Est. Fisica3","Fisica" , 580.0,7,80.86)]
        
        
    
    df = spark_session.createDataFrame(data,schema)
    actual_ds = GetTopByCareer(df,spark_session)
    
    
    expected_ds = spark_session.createDataFrame(
        [
            ("Est Medicina3","Medicina",795.0, 11,72.27,1),
            ( "Est Medicina1" ,"Medicina", 1544.0, 22 ,70.18,2),
            ( "Est. Fisica1", "Fisica" , 580.0,7,82.86,1),
            ("Est. Fisica3","Fisica" , 580.0,7,80.86,2)
        ],
        schemaResult)

    
    assert actual_ds.collect() == expected_ds.collect()
    
    print ("test_NormalDataGetTopByCareer OK")




def test_DuplicatesDataGetTopByCareer(spark_session):
    #probar total empate entre todos los estudiantes mostrar los 3
    schema = StructType([StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('Notas_Creditos', FloatType()),
                         StructField('creditos', IntegerType()),
                         StructField('Promedio', FloatType())])
    
    schemaResult = StructType([StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('Notas_Creditos', FloatType()),
                         StructField('creditos', IntegerType()),
                         StructField('Promedio', FloatType()),
                         StructField('Rank', IntegerType()),])
    
    data = [(  "Est Medicina1" ,"Medicina", 1544.0, 22 ,70.0), 
                ( "Est. Fisica1", "Fisica" , 580.0,7,70.0),
                ( "Est. Fisica2", "Fisica" , 580.0,7,70.0),
               ("Est Medicina2","Medicina",820.0, 10,   70.0),
               ("Est Medicina3","Medicina",795.0, 11,70.0),
               ("Est. Fisica3","Fisica" , 580.0,7,70.0)]
        
        
    
    df = spark_session.createDataFrame(data,schema)
    actual_ds = GetTopByCareer(df,spark_session)
    
    
    expected_ds = spark_session.createDataFrame(
       [(  "Est Medicina1" ,"Medicina", 1544.0, 22 ,70.0,1), 
        ("Est Medicina2","Medicina",820.0, 10,   70.0,1),
        ("Est Medicina3","Medicina",795.0, 11,70.0,1),
        ( "Est. Fisica1", "Fisica" , 580.0,7,70.0,1),
        ( "Est. Fisica2", "Fisica" , 580.0,7,70.0,1),
        ("Est. Fisica3","Fisica" , 580.0,7,70.0,1)],
        schemaResult)
    
    
    assert actual_ds.collect() == expected_ds.collect()
    
    print ("test_NormalDataGetTopByCareer OK")


def test_NullDataGetTopByCareer(spark_session):
    #dene retornar 2 filas para la carrer de fisica, aun sin  notas
    schema = StructType([StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('Notas_Creditos', FloatType()),
                         StructField('creditos', IntegerType()),
                         StructField('Promedio', FloatType())])

    
    data = [(  "Est Medicina1" ,"Medicina", 1544.0, 22 ,90.0), 
                ( "Est. Fisica1", "Fisica" , float('nan'),0,float('nan')),
                ( "Est. Fisica2", "Fisica" , float('nan'),0,float('nan')),
               ("Est Medicina2","Medicina",820.0, 10,   80.0),
               ("Est Medicina3","Medicina",795.0, 11,70.0)]
        
        
    
    df = spark_session.createDataFrame(data,schema)
    actual_ds = GetTopByCareer(df,spark_session)
    
    #retorna dos filas para la carrera de fisica??
    actual_ds=actual_ds.filter("carrera  == 'Fisica'")
    
    
    assert actual_ds.count() == 2
    
    print ("test_NullDataGetTopByCareer OK")



