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

## Staypoint Algorithm
Staypoint algorithm determines the stay point based on spatiotemporal data. Stay point is defined as the
place where user stays for a while. Stay point is bounded by two parameters i.e., T<sub>min</sub> and D<sub>max</sub>. The minimum time user must be in same place to consider it a stay point is given by T<sub>min</sub>. Whereas D<sub>max</sub> is the maximum distance between two consecutive places. By combining stay point with yelp data, one can determine the interesting venues in a region. 

   * Input data:- Spatiotempral RDF data, T<sub>min</sub>,D<sub>max</sub>
   * Output: stay points in a region

  
## Project Management
The project is managed using Maven in order to make it easily runnable in different platform. 
We provided a `run.sh` shell script to package and run the project with maven profile `dev`, and it should be easily 
adaptable.
