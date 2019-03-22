import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.streaming._


case class PageRankRes(uid : String, pr : Double);
case class LabelProRes(uid : String, community : String);

object NetWork{
  def main(argv:Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    //    val lines = ssc.socketTextStream("localhost", 4700)
    val sc = ssc.sparkContext
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val graph = GraphLoader.edgeListFile(sc, "relation.txt")
    val ranks = graph.pageRank(0.01).vertices
    val ranksDF = ranks.map(a=>PageRankRes(a._1.toString, a._2)).toDF

    val prop = new java.util.Properties
    prop.setProperty("serverTimezone", "UTC")
    prop.setProperty("user","root")
    prop.setProperty("password","971103")
    ranksDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf-8", "social", prop)


  }
}