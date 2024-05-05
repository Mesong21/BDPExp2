import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.nio.file.{Files, Paths}
import scala.collection.mutable.{Set, Stack, Map}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException

object SparkSCC {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SCC")
    val sc = new SparkContext(conf)

    // 读取图数据
    val edges = sc.textFile("file:///home/hadoop/sparkapp/SCC/edges.txt")
      .map(line => line.split(","))
      .map(parts => (parts(0).toInt, parts(1).toInt))
      .groupByKey()
      .mapValues(_.toList.sortBy(identity))
      .collectAsMap()
    val mutableEdges = collection.mutable.Map(edges.toSeq: _*)

    // DFS算法
    def dfs(v: Int, edges: collection.mutable.Map[Int, List[Int]]): (Set[Int], List[Int]) = {
      var visitedList = List[Int]()
      var visited = Set[Int]()
      val stack = Stack[Int](v)
      while (stack.nonEmpty) {
        val node = stack.pop()
        if (!visited.contains(node)) {
          visited += node
          visitedList = visitedList :+ node
          val neighbours: Iterable[Int] = edges.getOrElse(node, List())
          neighbours.foreach(stack.push)
        }
      }
      // println("visitedList: " + visitedList)
      (visited, visitedList)
    }
    
    // 计算每个节点的结束时间
    var finishTime = Map[Int, Int]()
    var time = 0
    var visited = Set[Int]()
    for ((node, _) <- edges if !visited.contains(node)) {
      val (_, visitedList) = dfs(node, mutableEdges)
      visitedList.reverse.foreach { node =>
        time += 1
        finishTime(node) = time
      }
    }
    println("finishTime: " + finishTime)
    // 对图进行转置
    val transposedEdges = sc.parallelize(edges.toSeq).flatMap { case (node, neighbours) =>
      neighbours.map((_, node))
    }
    .groupByKey()
    .mapValues(_.toList.sortBy(identity))
    .collectAsMap()
    val mutableTransposedEdges = collection.mutable.Map(transposedEdges.toSeq: _*)
    println("transposedEdges: " + mutableTransposedEdges)

    // 按照结束时间的递减顺序对转置后的图进行DFS
    visited = Set[Int]()
    var firstNode = 1
    if (finishTime.nonEmpty) {
      firstNode = finishTime.toList.sortBy(-_._2).head._1
    } else {
      println("No finish time found")
      // firstNode = 1
    }
    if (!visited.contains(firstNode)) {
      val (visitedComponent, _) = dfs(firstNode, mutableTransposedEdges)
      visited = visitedComponent
      println("Found SCC: " + visitedComponent.mkString(", "))
      val outputPath = "file:///home/hadoop/sparkapp/SCC/output"
      sc.parallelize(visitedComponent.toSeq).coalesce(1).saveAsTextFile(outputPath)
      println("Output saved")
    }
  }
}