from pyspark import SparkConf, SparkContext
def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames

def parseInput(line):
	fields = line.split()
	return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
	conf = SparkConf().setAppName("WorstMovies")
	sc = SparkContext(conf = conf)

	lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

	movieRatings = lines.map(parseInput)

	ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

	averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

	sortedMovies = averageRatings.sortBy(lambda x: x[1])

	results = sortedMovies.take(10)

	movieNames = loadMovieNames()

	for result in results:
		print(movieNames[result[0]], result[1])
