import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from graphframes import *

cwd = os.getcwd()
print(cwd)

edges_file = os.path.join(cwd, "citations.txt")
properties_file = os.path.join(cwd, "published-dates.txt")
years = [str(year) for year in range(1993, 2003)]

all_vertices = dict()
edges = list()
with open(edges_file) as f:
    for line in f:
        if line[0] != "#":
            vertices = line.split()
            for vertex in vertices:
                all_vertices[vertex] = None
            vertices.append("cites")
            edges.append(tuple(vertices))

# Associate vertices with their properties
with open(properties_file) as f:
    for line in f:
        if line[0] != "#":
            vertex_prop = line.split()
            # TODO: This does not handle cross-referenced papers yet
            if vertex_prop[0][:2] == "11":
                continue

            # Only interested in the year the paper was published
            vertex_prop[1] = vertex_prop[1][:4]
            if vertex_prop[0] in all_vertices:
                all_vertices[vertex_prop[0]] = tuple(vertex_prop)

# Create the spark context
spark = SparkSession.builder.appName('Assignment_1').getOrCreate()
sqlContext = SQLContext(spark)

# Create a Vertex DataFrame with unique ID column "id"
vertex_df = sqlContext.createDataFrame(list(all_vertices.values()), ["id", "published_year"])
# Create an Edge DataFrame with "src" and "dst" columns
edge_df = sqlContext.createDataFrame(edges, ["src", "dst", "relationship"])

# Create the graph
g = GraphFrame(vertex_df, edge_df)

# Analyze graphs
data = list()
for year in years:
    filtered_g = g.filterVertices(f"published_year <= {year}")
    num_vertices = filtered_g.vertices.count()
    num_out_edges = filtered_g.outDegrees.groupby().sum("outDegree").collect()
    data.append((num_vertices,
                 num_out_edges))

print(data)
