import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


case class PageRankRes(uid : String, pr : Double);
case class LabelProRes(uid : String, community : String);

object NetWork{
  def main(argv:Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._

  }
}