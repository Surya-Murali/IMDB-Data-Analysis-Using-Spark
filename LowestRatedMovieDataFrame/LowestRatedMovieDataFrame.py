#Using Datframe is much easier than Spark!
#Spark 2.0
#Import Spark Session - Encompasses SparkContext & SQLContext
from pyspark.sql import SparkSession
#Create dataframes from row objects
from pyspark.sql import Row
#SQL functions
from pyspark.sql import functions

#Map Movie IDs with Movie Names
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

#Split the input
#Return a row with movi and rating fields	
def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    #Create a SparkSession 
	#Create a new Spark Session or get a Spark session from where you left of last time
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    #Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    #Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    #Convert it to a RDD of Row objects with (movieID, rating)
    #Just extract MovieID and rating and create a RDD
	movies = lines.map(parseInput)
    #Convert that RDD to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    #Compute average rating for each movieID
	#Much easier
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    #Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    #Join the two together (We now have movieID, avg(rating), and count columns)
	#Joining 'counts' and 'averageRatings' datasets
    averagesAndCounts = counts.join(averageRatings, "movieID")

    #Pull the top 10 results
	#Order by 'avg(rating)' column
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    #Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    #Stop the session
    spark.stop()