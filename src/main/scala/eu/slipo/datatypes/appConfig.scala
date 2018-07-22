package eu.slipo.datatypes

case class Spark(master: String,
                 spark_serializer: String,
                 spark_executor_memory: String,
                 spark_driver_memory: String,
                 spark_driver_maxResultSize: String,
                 app_name: String)

case class Clustering(profile: String,
                      pic: String,
                      oneHotKM: String,
                      mdsKM: String,
                      word2VecKM: String,
                      picDistanceMatrix: String,
                      mdsCoordinates: String)

case class Datasets(input: String,
                    termValueUri: String,
                    termPrefix: String,
                    typePOI: String,
                    coordinatesPredicate: String,
                    categoryPOI: String,
                    poiPrefix: String)

case class appConfig(dataset: Datasets, clustering: Clustering, spark: Spark)