import matplotlib.pyplot as plt

vertices_edges_by_year = [[1993, 2117, 2919], [1994, 4421, 11519], [1995, 7276, 30055], [1996, 10481, 59236],
                          [1997, 14013, 98687], [1998, 17736, 143301], [1999, 21739, 201485], [2000, 21972, 204948],
                          [2001, 22811, 219189], [2002, 22938, 221412]]

paths_by_year = [[2117, 332, 146, 60], [4421, 1387, 877, 551], [7276, 3576, 2905, 2414], [10481, 6131, 5413, 4915],
                 [14013, 9058, 8273, 7723], [17736, 12173, 11325, 10719], [21739, 15561, 14752, 14245],
                 [21972, 15797, 14907, 14380], [22811, 17420, 16040, 15300], [22938, 17641, 16186, 15425]]

years = [x for x in range(1993, 2003)]
xs = [x for x in range(1, 5)]

# Plot vertex and edge data
fig, axs = plt.subplots(1)
path_xs = list()
path_ys = list()
for data in vertices_edges_by_year:
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
plt.savefig('vertices_and_edges.png', dpi=100)

# Plot path data
fig, axs = plt.subplots(len(paths_by_year))
fig.set_size_inches(18.5, 14.5)
for i, y in enumerate(paths_by_year):
    axs[i].plot(xs, y, "o-")
    axs[i].set_title(years[i])
    axs[i].set_xlabel("g(d)")
    axs[i].set_ylabel("# of Paths")

for i, ys in enumerate(paths_by_year):
    for x, y in zip(xs, ys):
        label = str(y)
        axs[i].annotate(label,  # this is the text
                        (x, y),  # these are the coordinates to position the label
                        textcoords="offset points",  # how to position the text
                        xytext=(0, 10),  # distance from text to points (x,y)
                        ha='center')  # horizontal alignment can be left, right or center
fig.tight_layout(pad=3.0)
plt.savefig('paths.png', dpi=100)
