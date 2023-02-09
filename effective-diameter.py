import networkx as nx

# Load the graph data
G = nx.read_edgelist("citations.txt")

# Initialize the g(d) list
gd = [0] * 21

# Calculate the shortest path for each node pair
for node in G.nodes():
    distances = nx.single_source_shortest_path_length(G, node)
    for distance in distances.values():
        if distance <= 20:
            gd[distance] += 1

# Calculate the cumulative distribution
total_pairs = gd[20]
for d in range(19, -1, -1):
    gd[d] += gd[d + 1]

# Find the effective diameter
for d in range(20):
    if gd[d] >= 0.9 * total_pairs:
        effective_diameter = d
        break

print("Effective diameter:", effective_diameter)