import networkx as nx
from scipy.sparse import coo_matrix
import numpy as np

# 创建一个有向图
G = nx.DiGraph()

# 添加一些边
G.add_edge(1, 2)
G.add_edge(2, 3)
G.add_edge(3, 4)
G.add_edge(2, 1)

# 获取所有的边
edges = G.edges()

# 保存边为文本文件
np.savetxt('DFS/edges.txt', edges, fmt='%d', delimiter=',')