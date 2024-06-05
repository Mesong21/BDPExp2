import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object GraphXDFSAndSCC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GraphX DFS and SCC").getOrCreate()
    val sc = spark.sparkContext
		val outputPath = "file:///share/sparkapp/graphxSCC/output"

    val edgefile = "file:///share/sparkapp/graphxSCC/data/graphx-wiki-edges.txt"
    val vertexfile = "file:///share/sparkapp/graphxSCC/data/graphx-wiki-vertices.txt"
    val testEdgefile = "file:///share/sparkapp/graphxSCC/data/test-edges.txt"
    val testVertexfile = "file:///share/sparkapp/graphxSCC/data/test-vertices.txt"
    // 读取数据
    val vertexData: RDD[(VertexId, String)] = sc.textFile(vertexfile)
      .map(line => {
        val fields = line.split("\t")
        (fields(0).toLong, fields(1))
      })

    val edgeData: RDD[Edge[Boolean]] = sc.textFile(edgefile)
      .map(line => {
        val fields = line.split("\t")
        Edge(fields(0).toLong, fields(1).toLong, true)
      })

    val graph = Graph(vertexData, edgeData)

   val dfsGraph = graph.outerJoinVertices(graph.connectedComponents().vertices) {
  case (id, _, comp) => comp.getOrElse(Long.MaxValue)
}.subgraph(epred = triplet => triplet.srcId < triplet.dstId)


	val sccGraph = graph.stronglyConnectedComponents(5)
	// 将强连通分量结果按照连通分量的标识符进行分组，并对每个分组内的顶点按照顶点标识符进行排序
	val sortedSCC = sccGraph.vertices.groupBy(_._2).mapValues(_.map(_._1).toList.sorted)
	dfsGraph.vertices.saveAsTextFile(outputPath + "/dfs")
	sccGraph.vertices.saveAsTextFile(outputPath + "/scc")
	sortedSCC.map{ case (id, vertices) => s"$id, ${vertices.mkString(",")}" }.saveAsTextFile(outputPath + "/sorted_scc")
	val multipleVerticesSCC = sortedSCC.filter(_._2.length > 1)
	multipleVerticesSCC.map{ case (id, vertices) => s"$id ->	${vertices.mkString(",")}" }.coalesce(1).saveAsTextFile(outputPath + "/multipled_scc")

    spark.stop()
  }

}