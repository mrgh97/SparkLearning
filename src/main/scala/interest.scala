import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

case class LikeSport(uid: String, interest: String)

object Interest{

  val sport="足球,篮球,羽毛球,乒乓球,网球,游泳,体操,跳水,高尔夫,攀岩,跳马,舞蹈,打球,踢球,玩水,鞍马,咱们"
  val sportstar="姚明,乔丹,霍华德,科比,菲尔普斯,李宁,梅西,张怡宁"
  val sportterm = (sport + "," + sportstar).split(",")

  var sMap = Map[String, Int]()
  for ( i <- 1 to sportterm.length ){
    sMap.+(sportterm(i-1)->1)
  }
  def containsSP(input : String)={
    var tag = false
    for ( i <- 1 to sportterm.length )
      if ( input.contains(sportterm(i-1)) )
        tag = true
    tag
  }

  def containsSPS(input : String)={
    sportterm.map(input.contains(_)).reduce(_||_)
  }

  def main(argv:Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    //    val lines = ssc.socketTextStream("localhost", 4700)
    val sc = ssc.sparkContext
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val up = sc.textFile("userprofile.txt").map(_.split("\t")).filter(_.length>12).map(a=>user(a(0))).toDF
    up.show

    val weibo = sc.textFile("weibo.txt").map(_.split("\t")).filter(_.length>5).filter(a=>containsSPS(a(5))).map(a=>(a(1),1)).reduceByKey(_+_).filter(_._2>5).map(a=> LikeSport(a._1,"sport")).toDF
    val resDF = up.join(weibo,"uid")
    val prop = new java.util.Properties
    prop.setProperty("serverTimezone", "UTC")
    prop.setProperty("user","root")
    prop.setProperty("password","971103")
    resDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf-8", "sport", prop)

  }
}
