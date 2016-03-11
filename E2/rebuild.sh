#!/bin/bash

export CLASSPATH=$(hadoop classpath)
export HADOOP_CLASSPATH=$CLASSPATH

javac -d classes DCacheJoinDictionaryMapper.java
javac -d classes DCacheJoinDictionaryReducer.java
jar -cvf DCacheJoin.jar -C classes/ .
javac -classpath $CLASSPATH:DCacheJoin.jar -d classes DCacheJoinDictionaryDriver.java
jar -uvf DCacheJoin.jar -C classes/ .
