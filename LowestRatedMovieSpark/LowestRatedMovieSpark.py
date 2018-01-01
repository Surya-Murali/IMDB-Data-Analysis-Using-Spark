#Import from pyspark library
from pyspark import SparkConf, SparkContext

#u.data
#UserID	MovID	Rating	Timestamp
#196	242		3		881250949
#186	302		3		891717742

#u.item
#MovID	MovTitle			ReleaseDate	VideoRel	IMDBLink
#1|		Toy Story (1995)|	01-Jan-1995|		|	http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0


#This function just creates a Python "dictionary" we can later
#It converts movie IDs to movie names while printing out the final results.
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
			#Maps Movie IDs with Movie Names
            movieNames[int(fields[0])] = fields[1]
    return movieNames

#Take each line of u.data and convert it to (movieID, (rating, 1.0))
#This way we can then add up all the ratings for each movie, and the total number of ratings for each movie (which lets us compute the average)
#Splits up the i/p data and returns back the str: key & value.
#Counting starts from 0 in Python
#We have the Key and the tuple of {rating & 1}
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

#Main Script
	if __name__ == "__main__":
		# The main script - create our SparkContext
		#Create a configuration class and sets the name as WorstMovies
		conf = SparkConf().setAppName("WorstMovies")
		#Create Spark context sc
		sc = SparkContext(conf = conf)

    #Load up our movie ID -> movie name lookup table
	#Its a dictionary called movieNames which has Movie IDs and Movie Names
    movieNames = loadMovieNames()

    #Load up the raw u.data file
	#HDFS path
	#RDD called lines
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    #Convert to (movieID, (rating, 1.0))
	#Create an RDD called movieRatings that contains a Key-Value pair
	#Key - Movie ID, Value : tupe of rating Value & "1"
	#Sum up all ratings & sum up all 1s to compute the average rating
    movieRatings = lines.map(parseInput)

    #Reduce to (movieID, (sumOfRatings, totalRatings))
	#Sum up all ratings and 1s using lambda function
	#Reduce operation : pass 2 values.
	#For each unique key, you pass in pair of values that have to be combined in some way.
	#Here we add up, the rating and 1
	#movie1 & movie2 are variables of the function
	#Reduce By Key. So it combines only 'Values' for each unique 'Key' and no change made to the 'Key'
	#KEY: movieID; VALUE: (sumOfRatings, totalRatings)
	#movie1[0] & movie1[0] indicates ratings.
	#movie2[0] & movie2[0] indicates '1'
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    #Map to (movieID, averageRating)
	#mapValues: Transforms the 'Values' (Finds the avg by dividing the first part of Key by its 2nd part) 
    #Getting the average
	averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    #Sort by average rating
	#Sort by the 2nd field : x[1]
	#Sorted (movieID, averageRating)
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    #Take the top 10 results
	#Top 10 (movieID, averageRating)
    results = sortedMovies.take(10)

    #Print them out:
    for result in results:
	#Using the movieNames dictionary
	#movieNames[result[0]] -> gives Movie Title
		#(MovieTitle, averageRating)
		print(movieNames[result[0]], result[1])
