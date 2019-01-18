#!/bin/bash
#mvn -Pdev clean
mvn  package -Pdev  -DskipTests && mkdir target/CORE
unzip target/CORE-bin.zip -d target/CORE
cd target/CORE && /home/rajjat/spark-2.3.1-bin-hadoop2.7/bin/spark-submit --jars=lib/*.jar --class eu.slipo.poi.stayPointAlgo slipo-0.0.1-SNAPSHOT.jar
