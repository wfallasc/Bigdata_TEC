#! /bin/bash
spark-submit join.py

spark-submit wfc.py


spark-submit tarea.py "estudiante.csv","cursos.csv","notas.csv"


from funciones import *