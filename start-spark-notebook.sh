#!/bin/bash

# NOTE this assumes some things about your set up - you may need to adjust
# JAVA_HOME and SPARK_HOME

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home
export SPARK_HOME=/opt/apache/spark-2.4.0
export PATH="${SPARK_HOME}/bin:$PATH"
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'

pyspark --executor-memory 4g --driver-memory 4g
