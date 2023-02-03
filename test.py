import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

cwd = os.getcwd()
print(cwd)

edges_file = os.path.join(cwd, "citations.txt")
vertex_file = os.path.join(cwd, "published-dates.txt")
years = [str(year) for year in range(1993, 2003)]

# Get vertices
vertices = set()
with open(vertex_file) as f:
    for line in f:
        if line[0] != "#":
            vertex_prop = line.split()
            # TODO: This does not handle cross-referenced papers yet. Because of this the number of nodes in the
            #  graph is incorrect!
            if vertex_prop[0][:2] == "11":
                vertex_prop[0] = vertex_prop[0][2:]

            vertices.add(tuple(vertex_prop))
print(vertices)