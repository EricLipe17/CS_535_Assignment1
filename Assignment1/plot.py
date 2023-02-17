# import matplotlib.pyplot as plt
#
# vertices_edges_by_year = [[1993, 2117, 2919],
# [1994, 4421, 11519],
# [1995, 7276, 30055],
# [1996, 10481, 59236],
# [1997, 14013, 98687],
# [1998, 17736, 143301],
# [1999, 21739, 201485],
# [2000, 21972, 204948],
#                           [2001, 22811, 219189],
#                           [2002, 22938, 221412]]
#
# paths_by_year = [[2117, 332, 146, 60], [4421, 1387, 877, 551], [7276, 3576, 2905, 2414], [10481, 6131, 5413, 4915],
#                  [14013, 9058, 8273, 7723], [17736, 12173, 11325, 10719], [21739, 15561, 14752, 14245],
#                  [21972, 15797, 14907, 14380], [22811, 17420, 16040, 15300], [22938, 17641, 16186, 15425]]

import os
import pandas as pd
import matplotlib.pyplot as plt
path_data = list()
vert_edge_data = list()
for root, dirs, files in os.walk("./output", topdown=True):
    dirs.sort()
    paths = dirs[10:]
    verts = dirs[:10]
    for path in paths:
        for r, d, fs in os.walk(os.path.join(root, path)):
            fs.sort(reverse=True)
            with open(os.path.join(r, fs[0]), 'r') as f:
                lines = f.readlines()
                path_data.append(int(lines[1]))
    year = 1993
    for vert_path in verts:
        for r, d, fs in os.walk(os.path.join(root, vert_path)):
            fs.sort(reverse=True)
            with open(os.path.join(r, fs[0]), 'r') as f:
                lines = f.readlines()
                splt = lines[1].split(',')
                splt = [year, int(splt[0]), int(splt[1])]
                vert_edge_data.append(splt)
        year += 1

g1 = path_data[0:10]
g2 = path_data[10:20]
g3 = path_data[20:30]
g4 = path_data[30:]
gs = [g1, g2, g3, g4]

df = pd.DataFrame({"g1": g1, "g2": g2, "g3": g3, "g4": g4})
print(df)
df.to_csv("gs.csv")


# Plot vertex and edge data
years = [x for x in range(1993, 2003)]
fig, axs = plt.subplots(1)
path_xs = list()
path_ys = list()
for data in vert_edge_data:
    path_xs.append(data[1])
    path_ys.append(data[2])
axs.plot(path_xs, path_ys, "o-")
for x, y, year in zip(path_xs, path_ys, years):
    label = str(year)
    axs.annotate(label,  # this is the text
                 (x, y),  # these are the coordinates to position the label
                 textcoords="offset points",  # how to position the text
                 xytext=(0, 10),  # distance from text to points (x,y)
                 ha='center')  # horizontal alignment can be left, right or center
fig.tight_layout(pad=3.0)
axs.set_xlabel("# of Vertices")
axs.set_ylabel("# of Edges")
plt.savefig('vertices_and_edges_test.png', dpi=100)
