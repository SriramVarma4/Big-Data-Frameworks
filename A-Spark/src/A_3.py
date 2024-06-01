from pyspark import SparkContext, SparkConf
import heapq

def parse_edges(line):
    parts = line.split('\t')
    return int(parts[0]), int(parts[1])

def initialize_spark(app_name):
    conf = SparkConf().setAppName(app_name)
    return SparkContext(conf=conf)

def load_graph(sc, filepath):
    data = sc.textFile(filepath).zipWithIndex().filter(lambda x: x[1] >= 4).map(lambda x: x[0])
    edges = data.map(parse_edges)
    return edges

def build_graph(edges):
    return edges.groupByKey().mapValues(list).cache()

def broadcast_graph(sc, graph):
    graph_dict = dict(graph.collect())
    return sc.broadcast(graph_dict)

def shortest_path(graph, start_node):
    visited = {start_node: ([start_node], 0)}
    priority_queue = [(0, start_node, [start_node])]

    while priority_queue:
        current_distance, current_node, current_path = heapq.heappop(priority_queue)

        for neighbor in graph.value.get(current_node, []):
            new_distance = current_distance + 1
            if neighbor not in visited or new_distance < visited[neighbor][1]:
                visited[neighbor] = (current_path + [neighbor], new_distance)
                heapq.heappush(priority_queue, (new_distance, neighbor, current_path + [neighbor]))

    return visited

def format_output(source_node, shortest_paths):
    return [
        f"{source_node} | {destination_node} | {', '.join(map(str, path))} | {cost}"
        for destination_node, (path, cost) in shortest_paths.items()
    ]

def main():
    sc = initialize_spark("ShortestPaths")
    source_node = 17274
    
    edges = load_graph(sc, "ca-GrQc.txt")
    edges.cache()

    graph = build_graph(edges)
    graph_broadcast = broadcast_graph(sc, graph)

    shortest_paths = shortest_path(graph_broadcast, source_node)

    output = format_output(source_node, shortest_paths)
    sc.parallelize(output).coalesce(1).saveAsTextFile("output")
    
    sc.stop()

if __name__ == "__main__":
    main()
