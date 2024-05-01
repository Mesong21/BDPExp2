import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.nio.file.{Files, Paths}

object SparkDFS {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DFS")
    val sc = new SparkContext(conf)

    // 读取图数据
    val edges = sc.textFile("file:///home/hadoop/sparkapp/DFS/edges.txt")
      .map(line => line.split(","))
      .map(parts => (parts(0).toInt, parts(1).toInt))
      .groupByKey()
      .mapValues(_.toList.sortBy(identity))
      .collectAsMap()
    // 打印图数据
    edges.foreach(println)
    // DFS算法
    def dfs(v: Int, visited: Set[Int]): Set[Int] = {
      if (visited.contains(v))
        visited
      else {
        val neighbours: Iterable[Int] = edges.getOrElse(v, List())
        neighbours.foldLeft(visited + v)((b, a) => dfs(a, b))
      }
    }

    // 从节点1开始进行DFS
    val visited = dfs(1, Set())
    println("Visited nodes: " + visited.mkString(", "))
    // 将结果保存到文件
    val outputPath = Paths.get("file:///home/hadoop/sparkapp/DFS/output")
    if (Files.exists(outputPath)) {
      Files.delete(outputPath)
      println("Output deleted")
    }
    sc.parallelize(visited.toSeq).coalesce(1).saveAsTextFile("file:///home/hadoop/sparkapp/DFS/output")
    println("Output saved")
  }
}