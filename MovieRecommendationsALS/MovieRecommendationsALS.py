#Recommend movies for a given user based on the other movies the user has watched
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

#Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

#Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
	#Return rows containing UserID, MovieID and the Rating
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == "__main__":
    #Create a SparkSession (the config bit is only for Windows!)
	#Call it 'MovieRecs'
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    #Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    #Get the raw data
	#Get the RDD of lines
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    #Convert it to a RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    #Convert to a DataFrame and cache it( to use the dataframe more than once )
	#Dataframe of userID, movieID, rating
   	ratings = spark.createDataFrame(ratingsRDD).cache()

    #Create an ALS collaborative filtering model from the complete data set
	#Create an ALS model
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    #Train the ALS model on the entire dataset and give that model back
	model = als.fit(ratings)

	#Movie recommendations for User ID 0
    #Print out ratings from user 0:
    print("\nRatings for user ID 0:")
	#Filter out the dataset
    userRatings = ratings.filter("userID = 0")
	#Iterate through userRatings and print the results
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']

	#Prediction	
    print("\nTop 20 recommendations:")
    #Restrict to only movies that were rated more than 100 times
	#Grouping the dataset by MovieID and counting them up. Also filtered
	#So we have a dataframe of MovieID, the no. of times it was rated (only contains MovieIDs rated > 100 times)
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    
	#Test Dataframe which has 2 columns - MovieIDs(All MovieIDs with >100 ratings) and adding a new column : User ID with a value of 0
	#Go through the MovieIDs and predict the rating that userID 0 would have for that movie
	#Construct a "test" dataframe for user 0 with every movie rated more than 100 times
	popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

    #Run our model on that list of popular movies for user ID 0
	#Pass that dataframe into the model.transform function
    recommendations = model.transform(popularMovies)

    #Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])

    spark.stop()