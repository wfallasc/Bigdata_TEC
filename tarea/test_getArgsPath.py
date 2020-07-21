from funciones import *


def test_getPathsWithoutArgs():
    #valida si la funcion de obtener parametros de consola no falle, debe devolver los path por defecto
    path1,path2,path3 =  getArgsPath()

    assert (len(path1) > 0) &   (len(path2) > 0) &  (len(path3) > 0)
    print ("test_getPathsWithoutArgs OK")
