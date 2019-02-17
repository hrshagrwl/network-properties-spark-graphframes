import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

# Function to return Connected Component Count
def getCCC(g):
	return g.connectedComponents().select('component').distinct().count()

def articulations(g, usegraphframe=False):
	# Get the starting count of connected components
	baseCount = getCCC(g)
	# Default version sparkifies the connected components process 
	# and serializes node iteration.
	output = []
	# Get vertex list for serial iteration
	vertices = g.vertices.rdd.map(lambda x: x[0]).collect()
	edges = g.edges.rdd.map(lambda x: (x[0], x[1])).collect()
	if usegraphframe:
		# For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the output
		for v in vertices:
			r_v = g.vertices.filter('id != "' + v + '"')
			r_e = g.edges.filter('src != "' + v +'"').filter('dst != "' + v +'"')
			temp_g = GraphFrame(r_v, r_e)
			count = getCCC(temp_g)
			output.append((v, 1 if count > baseCount else 0))
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
	else:
		new_g = nx.Graph()
		new_g.add_nodes_from(vertices)
		new_g.add_edges_from(edges)

		def connectedComponents(v):
			temp_g = deepcopy(new_g)
			temp_g.remove_node(v)
			return nx.number_connected_components(temp_g)

		for v in vertices:
			count = connectedComponents(v)
			output.append((v, 1 if count > baseCount else 0))
	
	return sqlContext.createDataFrame(sc.parallelize(output), ['id', 'articulation'])


filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)

df.filter('articulation = 1').toPandas().to_csv('articulations_out.csv')

print("---------------------------")

#Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)

df.filter('articulation = 1').toPandas().to_csv('graphframe_true.csv')
