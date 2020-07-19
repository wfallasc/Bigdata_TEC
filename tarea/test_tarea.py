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
    
    expression =  student_empty.Carnet==grades_empty.carnet
    my_emptyResult = Join_StudentGrades('Bla bla bla',grades_empty, expression,spark_session)

    assert my_emptyResult.count() == 0
    print ("test_NoValidStudentParameter OK")


def test_NoValidGradesParameter(spark_session):
    
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



def test_Add_calculatedColums(spark_session):
    
    #validar si la nueva columna calculada existe en el datraframe
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
    
    my_emptyResult = AddCalculatedColumns('',spark_session)
    
    assert my_emptyResult.count() == 0
    print ("test_NoValidParameterAddCalculatedColum OK")



def test_Add_calculatedColumWithNullValues(spark_session):
    
    #validar si la nueva columna calculada existe en el datraframe
    StudentGrades_data = [(1,'test student 1', 'medicina',1,1,70,1,'null','null'), 
                          (1,'test student 1', 'medicina',1,1,80,1,'null','null')]
    StudentGrade_ds = spark_session.createDataFrame(StudentGrades_data,
                                              ['carnet', 'nombre','carrera','carnet','cod_curso','nota','cod_curso',
                                              'creditos','carrera'])

    
    actual_ds = AddCalculatedColumns(StudentGrade_ds,spark_session)
    
    
    assert actual_ds.count() == 2
    
    print ("test_Add_calculatedColumWithNullValues OK")



def test_NoValidParameterGetGroupByNameAndCareer(spark_session):
    
    my_emptyResult = GetGroupByNameAndCareer('',spark_session)
    
    assert my_emptyResult.count() == 0
    print ("test_NoValidParameterGetGroupByNameAndCareer OK")


def test_GetGroupByNameAndCareer_NoValidCreditos(spark_session):
    # check possible division by 0 in sum creditos
    data = [(1,'test student 1', 'medicina',1,1,70,1,0,'medicina',0), 
                          (2,'test student 2', 'medicina',1,1,80,1,0,'medicina',0)]
    df = spark_session.createDataFrame(data,
                                              ['carnet', 'nombre','carrera','carnet_curso','cod_curso','nota','cod_curso',
                                              'creditos','carrera_curso','Notas_Creditos'])
    actual_ds = GetGroupByNameAndCareer(df,spark_session)
    
    assert actual_ds.count() == 2
    
    print ("test_GetGroupByNameAndCareer_emptyFrame OK")

def test_GetGroupByNameAndCareer_EmpyFrame(spark_session):
    # check possible division by 0 in sum creditos
    
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


def test_getPathsWithoutArgs():
    
    path1,path2,path3 =  getArgsPath()

    assert (len(path1) > 0) &   (len(path2) > 0) &  (len(path3) > 0)
    print ("test_getPathsWithoutArgs OK")


def test_NoValidParameterGetTopByCareer(spark_session):
    
    my_emptyResult = GetTopByCareer('',spark_session)
    
    my_emptyResult.show()
    
    assert my_emptyResult.count() == 0
    print ("test_NoValidParameterGetTopByCareer OK")

def test_EmptyDFGetTopByCareer(spark_session):
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
    

    actual_ds=actual_ds.filter("carrera  == 'Fisica'")
    
    
    assert actual_ds.count() == 2
    
    print ("test_NullDataGetTopByCareer OK")
