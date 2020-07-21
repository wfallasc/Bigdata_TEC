from funciones import *

from pyspark.sql import functions as F
import  pyspark.sql.column
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType,FloatType
from pyspark.sql.functions import col, udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


def test_student_without_course(spark_session):
    
    #validar left join, estudiante id=2 no ha llevado cursos, el dataframe final deberia tener
    #3 filas
    grades_data = [(1,1, 50), (1,2, 100)]
    grades_ds = spark_session.createDataFrame(grades_data,
                                              ['carnet', 'cod_curso','nota'])
    
    student_data = [(1, 'Juan','Medicina'), (2, 'Maria','Quimica')]
    student_ds = spark_session.createDataFrame(student_data,
                                               ['Carnet', 'nombre','carrera'])
    
    expression =  student_ds.Carnet==grades_ds.carnet
    
    actual_ds = Join_StudentGrades(student_ds,grades_ds, expression,spark_session)
    
    assert actual_ds.count() == 3
    print ("test_student_without_course OK")





def test_joinWithEmpyGradesFrame(spark_session):
    #validar left join, dataframe estudiante con dos filas, grades vacio, se espera un dataset con dos filas

    schema_grades = StructType([StructField('carnet', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
    
    grades_ds=spark_session.createDataFrame([], schema_grades)
    
    student_data = [(1, 'Juan','Medicina'), (2, 'Maria','Quimica')]
    student_ds = spark_session.createDataFrame(student_data,
                                               ['Carnet', 'nombre','carrera'])
    
    expression =  student_ds.Carnet==grades_ds.carnet
    
    actual_ds = Join_StudentGrades(student_ds,grades_ds, expression,spark_session)
    
    assert actual_ds.count() == 2
    print ("test_joinWithEmpyGradesFrame OK")



def test_joinTwoEmptyFrames(spark_session):
    
    #validar left join entre dos dataframes vacios, deberia retornar 0 filas y no caerse
    schema_grades = StructType([StructField('carnet', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
    
    grades_empty=spark_session.createDataFrame([], schema_grades)
    
    
    schema_student = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType())])
    student_empty = spark_session.createDataFrame([],
                                               schema_student)
    
    expression =  student_empty.Carnet==grades_empty.carnet
    
    my_emptyResult = Join_StudentGrades(student_empty,grades_empty, expression,spark_session)

    assert my_emptyResult.count() == 0
    print ("test_joinTwoEmptyFrames OK")


def test_NoValidWhereParameter(spark_session):
    
    #validar que al mandar cualquier parametro en el where no bote la funcion
    schema_grades = StructType([StructField('carnet', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
    
    grades_empty=spark_session.createDataFrame([], schema_grades)
    
    
    schema_student = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType())])
    student_empty = spark_session.createDataFrame([],
                                               schema_student)

    my_emptyResult = Join_StudentGrades(student_empty,grades_empty, '',spark_session)

    assert my_emptyResult.count() == 0
    print ("test_NoValidWhereParamter OK")


def test_NoValidStudentParameter(spark_session):
    
    #validar que al mandar cualquier parametro en el dataframe de estudiante no bote la funcion
    schema_grades = StructType([StructField('carnet', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
    
    grades_empty=spark_session.createDataFrame([], schema_grades)
    
    schema_student = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType())])
    student_empty = spark_session.createDataFrame([],
                                               schema_student)
    
    expression =  student_empty.Carnet==grades_empty.carnet
    my_emptyResult = Join_StudentGrades('Bla bla bla',grades_empty, expression,spark_session)

    assert my_emptyResult.count() == 0
    print ("test_NoValidStudentParameter OK")



def test_NoValidGradesParameter(spark_session):
    
    #validar que al mandar un parametro no valido en el dataset de notas no bote la funcion, 
    #debe retornar un dataframe vacio 
    schema_grades = StructType([StructField('carnet', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
    
    grades_empty=spark_session.createDataFrame([], schema_grades)
    
    
    schema_student = StructType([StructField('Carnet', IntegerType()),
                         StructField('nombre', StringType()),
                         StructField('carrera', StringType())])
    student_empty = spark_session.createDataFrame([],
                                               schema_student)
    
    expression =  student_empty.Carnet==grades_empty.carnet
    my_emptyResult = Join_StudentGrades(student_empty,'', expression,spark_session)

    assert my_emptyResult.count() == 0
    print ("test_NoValidGradesParameter OK")


def test_full_join_StudentGrades_Courses(spark_session):
    
    #validar full join, un estudiante id=1 con dos cursos, deberia resultar en un dataset de 2 filas
    StudentGrades_data = [(1,'test student 1', 'medicina',1,1,70), (1,'test student 1', 'medicina',1,2,80)]
    StudentGrade_ds = spark_session.createDataFrame(StudentGrades_data,
                                              ['carnet', 'nombre','carrera','carnet','cod_curso','nota'])
    

    schema_course = StructType([StructField('cod_curso', IntegerType()),
                         StructField('creditos', StringType()),
                         StructField('carrera', StringType())])
    
    course_data = [(1, 4,'Medicina'), (2, 2,'Quimica')]
    course_ds = spark_session.createDataFrame(course_data,schema_course)
    
    expression =  course_ds.cod_curso==StudentGrade_ds.cod_curso
    
    actual_ds = Join_StudentGrades(StudentGrade_ds,course_ds, expression,spark_session)

    assert actual_ds.count() == 2
    print ("test_full_join_StudentGrades_Courses OK")




def test_JoinTemporalUnionsStudentswithNoCourses(spark_session):
    #validar join de estudianates que no ha llevado cursos, debe retornar el estudiante con la nota en nulo

    schemaStudents = StructType([StructField('Carnet', IntegerType()),
                          StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('carnet_curso', IntegerType()),
                         StructField('cod_curso', IntegerType())])
    
    schemaGrades = StructType([StructField('carnet_curso', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])
    
    dataStudent = [( 1 ,"Ociquito Bravo", "Medicina", 1 ,1), 
                    ( 2, "Yoyito" , "Fisica",0,0)]
    

    #yoyito no tiene notas  en la carrera de fisica
    DataGrades= [( 1 ,1, 70.0)]
    
    dfStudent = spark_session.createDataFrame(dataStudent,schemaStudents)
    dfGrades = spark_session.createDataFrame(DataGrades,schemaGrades)
    
    expression =  dfStudent.Carnet==dfGrades.carnet_curso
    dfUnion = Join_StudentGrades(dfStudent,dfGrades,expression,spark_session)

    
    schemaResult = StructType([StructField('Carnet', IntegerType()),
                          StructField('nombre', StringType()),
                         StructField('carrera', StringType()),
                         StructField('carnet_curso', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('carnet_curso', IntegerType()),
                         StructField('cod_curso', IntegerType()),
                         StructField('nota', FloatType())])

    
    expected_ds = spark_session.createDataFrame(
        [
            (1,"Ociquito Bravo","Medicina",1, 1,1,1,70.0),
            (2,"Yoyito" ,"Fisica", 0, 0 , None, None, None)
        ],schemaResult)

    
    assert dfUnion.collect() == expected_ds.collect()
    
    print ("test_JoinTemporalUnionsStudentswithNoCourses OK")
