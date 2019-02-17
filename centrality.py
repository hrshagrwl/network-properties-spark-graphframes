from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from pyspark.sql.functions import explode

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def closeness(g):
	
	# Get list of vertices. We'll generate all the shortest paths at
	# once using this list.
	# https://stackoverflow.com/questions/32000646/extract-column-values-of-dataframe-as-list-in-apache-spark
	vertices = g.vertices.rdd.map(lambda x: x.id).collect()

	# first get all the path lengths.
	# Break up the map and group by ID for summing
	# Sum by ID
	# https://stackoverflow.com/questions/37898313/convert-graphframes-shortestpath-map-into-dataframe-rows-in-pyspark
	paths = g.shortestPaths(landmarks = vertices)
	exploded = paths.select('id', explode('distances')).rdd.map(lambda x: (x[0], float(x[2])))
	# Get the inverses and generate desired dataframe.

	inversed = exploded.reduceByKey(lambda a,b: a + b).map(lambda x: (x[0], 1/x[1]))
	return sqlContext.createDataFrame(inversed, ['id', 'closeness'])

print("Reading in graph for problem 2.")
graph = sc.parallelize([('A','B'),('A','C'),('A','D'),
	('B','A'),('B','C'),('B','D'),('B','E'),
	('C','A'),('C','B'),('C','D'),('C','F'),('C','H'),
	('D','A'),('D','B'),('D','C'),('D','E'),('D','F'),('D','G'),
	('E','B'),('E','D'),('E','F'),('E','G'),
	('F','C'),('F','D'),('F','E'),('F','G'),('F','H'),
	('G','D'),('G','E'),('G','F'),
	('H','C'),('H','F'),('H','I'),
	('I','H'),('I','J'),
	('J','I')])
	
e = sqlContext.createDataFrame(graph,['src','dst'])
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()
print("Generating GraphFrame.")
g = GraphFrame(v,e)

print('Calculating closeness.')
df = closeness(g).sort('closeness',ascending=False)

df.toPandas().to_csv('centrality_out.csv')
