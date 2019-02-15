from pyspark import SparkConf, SparkContext
import sys
import csv

# comparator function to sort the list
def compare(item1, item2):
	i1 = item1.split(",")
	i2 = item2.split(",")
	if i1[2] < i2[2]:
		return -1
	elif i1[2] > i2[2]:
		return 1
	else:
		return i1[-1] > i2[-1]


if __name__ == '__main__':

	if len(sys.argv) != 3:
		print 'Incorrect Number of Arguments'
		exit(-1) 

	fileName = sys.argv[1]
	output = sys.argv[2]

	# Set spark configurations
	conf = (SparkConf().setAppName('CS744Assignment1Part2')\
						.set("spark.executor.memory", "8g")\
						.set("spark.driver.memory", "8g")\
						.set("spark.executor.cores", "5")\
						.set("spark.task.cpus", 1)\
						)

	# Initialize the spark context.
	sc = SparkContext(conf = conf)

	# Load the input file 
	# lines = sc.textFile("hdfs://128.104.223.117:9000/user/test/export.csv")
	lines = sc.textFile(fileName)

	res = lines.collect()

	#sort the file content
	res[1:] = sorted(res[1:], cmp=compare)

	#store the output in hdfs
	sc.parallelize(res).saveAsTextFile(output)

	# sc.parallelize(res).saveAsTextFile("hdfs://128.104.223.117:9000/user/test/output10.csv")
	# write_list_to_file(res, "output.csv")

	sc.stop()