# SANSA-POI

This repository contains code to process RDF dataset in SLIPO project which contains a certain amount of 
Point of Interest(POI). Furthermore, we did some clustering analysis of the data.

## Clustering Algorithms
We are mainly using Power Iteration and K-Means clustering algorithms to provide clustering analysis 
based on the categories of POI. The clustering algorithms are from Spark standard ML library and 
we developed several encoding methods to encode categorical data to numerical.

1. OneHot Encoding
2. Word2Vec Encoding (from Spark ML)
3. Multidimensional Scaling Encoding (Third Party Library, see pom.xml)


## Project Management
The project is managed using Maven in order to make it easily runnable in different platform. 
We provided a `run.sh` shell script to package and run the project with maven profile `dev`, and it should be easily 
adaptable.
