import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.nio.file.{Files, Paths}
import scala.collection.mutable.{Set, Stack}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException

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
    // edges.foreach(println)
    // DFS算法
    def dfs(v: Int): (Set[Int], List[Int]) = {
      var visitedList = List[Int]()
      var visited = Set[Int]()
      val stack = Stack[Int](v)
      while (stack.nonEmpty) {
        val node = stack.pop()
        if (!visited.contains(node)) {
          visited += node
          // println("Visiting node " + node)
          visitedList = visitedList :+ node
          val neighbours: Iterable[Int] = edges.getOrElse(node, List())
          neighbours.foreach(stack.push)
        }
      }
      (visited, visitedList)
    }

    // 从节点1开始进行DFS
    val (visited, visitedList) = dfs(1)
    println("Visited nodes: " + visitedList.mkString(", "))
    // 将结果保存到文件
    val outputPath = "file:///home/hadoop/sparkapp/DFS/output"
    
    sc.parallelize(visitedList.toSeq).coalesce(1).saveAsTextFile(outputPath)
    println("Output saved")
  }
}