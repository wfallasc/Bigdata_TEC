import sys
import  pyspark.sql.column
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType,FloatType
from pyspark.sql.functions import col, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

def getArgsPath():
    
    Student_path= "estudiante.csv"
    Course_path= "cursos.csv"
    Grades_path= "notas.csv"
    try:
        if len(sys.argv)>0:
            csvPath= str(sys.argv[1])
            ls = csvPath.split(",")
            if (len(ls)>2):
                St_path = ls[0]
                Co_path  = ls[1]
                Gra_path  = ls[2]
                return St_path,Co_path,Gra_path
    except:
        print("Rutas no validas espeficadas en args")
                
    return Student_path,Course_path,Grades_path


def Get_StudentsDF(spark,Student_path):
    
    #"""Return the spark dataframe of given student path. if failed, the return an empty dataframe""
    schema_student = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType())])
    try:
        

        dataframe = spark.read.csv(Student_path,
                           schema=schema_student,
                           header=True)
        
        return  dataframe
        
    except:
          print("Error cargando los datos de estudiante en la ruta especificada")
          return  spark.createDataFrame([], schema_student)


def Get_CoursesDF(spark,Course_path):
    
    #"""Return the spark dataframe of given course path. if failed, the return an empty dataframe""
    schema_course = StructType([StructField('codcurso', IntegerType()),
                         StructField('creditos', IntegerType()),
                         StructField('carrera_curso', StringType())])
    
    try:
        

        dataframe = spark.read.csv(Course_path,
                           schema=schema_course,
                           header=True)
        
        return  dataframe
        
    except:
          print("Error cargando los datos de cursos en la ruta especificada")
          return  spark.createDataFrame([], schema_course)


def Get_GradesDF(spark,Grades_path):
    
    #"""Return the spark dataframe of given grades path. if failed, the return an empty dataframe""
    schema_grades = StructType([StructField('carnet_curso', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
 
        
    try:
        

        dataframe = spark.read.csv(Grades_path,
                           schema=schema_grades,
                           header=True)
        
        return  dataframe
        
    except:
          print("Error cargando los datos de notas en la ruta especificada")
          return  spark.createDataFrame([], schema_grades)



def Join_StudentGrades(Df_Students,Df_Grades,where,spark):
    #"""Return the spark dataframe of left outer join.""
    
    schema = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('carnet', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
        
    if not(isinstance(where, pyspark.sql.column.Column)) :
        print("No valid where parameter")
        return spark.createDataFrame([], schema)
    
    if not(isinstance(Df_Students, pyspark.sql.dataframe.DataFrame)) :
        print("No valid Df_Students parameter")
        return spark.createDataFrame([], schema)
    
    if not(isinstance(Df_Grades, pyspark.sql.dataframe.DataFrame)) :
        print("No valid Df_Grades parameter")
        return spark.createDataFrame([], schema)
    
    df = Df_Students.join(Df_Grades, where, 'left_outer')
    return df


def AddCalculatedColumns(Df_Union,spark):
    #"""Add calculted columns for futher analisys.""
    
    schema = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('carnet_curso', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType()),
                        StructField('cod_curso', IntegerType()),
                        StructField('creditos', IntegerType()),
                        StructField('carrera_curso', StringType()),
                        StructField('Notas_Creditos', FloatType())])
    
    if not(isinstance(Df_Union, pyspark.sql.dataframe.DataFrame)) :
        print("No valid where parameter")
        return spark.createDataFrame([], schema)
    
    Df_Union = Df_Union.withColumn("Notas_Creditos",
                                    col("nota") *  col("creditos"))
    
    
    return Df_Union


def GetGroupByNameAndCareer(dfUnion,spark):
    #group by nombre and carrera, add calculated columns
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
    
    
    if not(isinstance(dfUnion, pyspark.sql.dataframe.DataFrame)) :
        print("No valid dataframe")
        return spark.createDataFrame([], schema)
    
    Df_GroupNotas = dfUnion.groupBy("nombre","carrera").sum("Notas_Creditos","creditos")

    Df_GroupNotas= Df_GroupNotas.withColumn("Promedio",
                                        col("sum(Notas_Creditos)") /  col("sum(creditos)"))

    Df_GroupNotas = Df_GroupNotas.withColumn("Promedio", func.round(Df_GroupNotas["Promedio"], 2))
    
    return Df_GroupNotas


def GetTopByCareer(Df_Grades,spark):
    #"""add rank by promedio, then select the ones <= 2
    
    schema = StructType([StructField('nombre', IntegerType()),
                         StructField('carrera', StringType()),
                         StructField('Promedio', IntegerType())])
    
    if not(isinstance(Df_Grades, pyspark.sql.dataframe.DataFrame)) :
        print("No valid dataframe")
        return spark.createDataFrame([], schema)
        
    window = Window.partitionBy(Df_Grades['carrera']).orderBy(Df_Grades['Promedio'].desc())

    Df_Grades=Df_Grades.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 2) 
    
    Df_Grades.orderBy("carrera")
    
    return Df_Grades
