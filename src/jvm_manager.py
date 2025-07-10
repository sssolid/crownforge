import jpype
import os

def start_jvm_once(jars):
    if not jpype.isJVMStarted():
        classpath = [os.path.abspath(jar) for jar in jars]
        jpype.startJVM(classpath=classpath, convertStrings=True)