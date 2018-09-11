#!/bin/bash
mvn clean
mvn package -Pdev && mkdir target/CORE
unzip target/CORE-bin.zip -d target/CORE
cd target/CORE && /home/yakun/softwares/spark-2.3.1-bin-hadoop2.7/bin/spark-submit --executor-memory 10000mb  --jars=lib/*.jar --class eu.slipo.poi.poiClustering slipo-0.0.1-SNAPSHOT.jar
