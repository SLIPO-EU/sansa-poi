package eu.slipo.datatypes

/**
 * Poi object representing a point of interest
 * 
 * @param poi_id, id of poi
 * @param coordinate, coordinate of poi
 * @param categories, categories of poi
 */
case class Poi(poi_id: Long, coordinate: Coordinate, categories: Categories)