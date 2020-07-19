import sys
from funciones import *

import  pyspark.sql.column
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType,FloatType
from pyspark.sql.functions import col, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col



print("Init programa tarea-----------------------------")

Student_path,Course_path,Grades_path = getArgsPath()


spark = SparkSession.builder.appName("LoadData").getOrCreate()

#load dataframes ------------------------
Df_Students = Get_StudentsDF(spark,Student_path)

Df_Courses = Get_CoursesDF(spark,Course_path)

Df_Grades = Get_GradesDF(spark,Grades_path)
#----------------------------------------

#join students and grades----------------------
expression =  Df_Students.Carnet==Df_Grades.carnet_curso
dfUnion = Join_StudentGrades(Df_Students,Df_Grades,expression,spark)
#------------------------------------------------

#join Union dataframe with courses -------------
expression =  dfUnion.cod_curso==Df_Courses.codcurso
dfUnion = Join_StudentGrades(dfUnion,Df_Courses,expression,spark)
#------------------------------------------------

dfUnion=AddCalculatedColumns(dfUnion,spark)

#todo add test cases for this
Df_GroupNotas =GetGroupByNameAndCareer(dfUnion,spark) 

#todo add test cases for this
DsFinal = GetTopByCareer(Df_GroupNotas,spark)

cols = ['carrera','nombre','Promedio']
DsFinal =  DsFinal.select(*cols)

DsFinal.show()



print("End program Tarea------------------------------")
