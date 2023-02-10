import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import sum

cwd = os.getcwd()
print(cwd)

edges_file = os.path.join(cwd, "citations.txt")
vertex_file = os.path.join(cwd, "published-dates.txt")
years = [str(year) for year in range(1993, 2003)]

# Get all edges
edges = list()
vertices_dict = dict()
with open(edges_file) as f:
    for line in f:
        if line[0] != "#":
            e = line.split()
            e.append("cites")
            edges.append(tuple(e))
            vertices_dict[e[0]] = None
            vertices_dict[e[1]] = None

# Get vertices
vertices = set()
with open(vertex_file) as f:
    for line in f:
        if line[0] != "#":
            vertex_prop = line.split()
            # TODO: This does not handle cross-referenced papers yet. Because of this the number of nodes in the
            #  graph is incorrect according to citations.txt!
            if vertex_prop[0][:2] == "11":
                vertex_prop[0] = vertex_prop[0][2:]

            vertex_prop[1] = vertex_prop[1][:4]
            if vertex_prop[0] in vertices_dict:
                vertices_dict[vertex_prop[0]] = vertex_prop[1]

for key, val in vertices_dict.items():
    vertices.add((key, val))

# Create the spark context
spark = SparkSession.builder.appName('Assignment_1').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sqlContext = SQLContext(spark)

# Create a Vertex DataFrame with unique ID column "id"
vertex_df = sqlContext.createDataFrame(vertices, ["id", "published_year"])
# Create an Edge DataFrame with "src" and "dst" columns
edge_df = sqlContext.createDataFrame(edges, ["src", "dst", "relationship"])

data = list()
num_vertices = 0
for year in years:
    # Collect number of vertices by year
    vertices_by_year_df = vertex_df.filter(vertex_df.published_year <= year)
    num_vertices = vertices_by_year_df.count()
    print(vertices_by_year_df.count())

    # Collect number of out edges by year
    edges_by_year = vertices_by_year_df.join(edge_df, vertices_by_year_df.id == edge_df.src, "inner")
    num_edges = edges_by_year.groupby(edges_by_year.src).count().select(sum("count")).collect()[0][0]
    print(edges_by_year.count())

    # Store year and counts
    data.append((year, num_vertices, num_edges))
    print(data)

print(f"V: {vertex_df.count()}, E: {edge_df.count()}")
print(data)
