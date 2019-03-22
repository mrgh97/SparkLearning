import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

case class LikeFootball(uid : String, insterest : String)

object Interest2{

  val footballteam = "曼联,利物浦,阿森纳,切尔西,曼彻斯特城,托特纳姆热刺,埃弗顿,巴塞罗那,皇家马德里,瓦伦西亚,马德里竞技,马拉加,尤文图斯,AC米兰,国际米兰,罗马,那不勒斯,拉齐奥,乌迪内斯,佛罗伦萨,拜仁慕尼黑,多特蒙德,勒沃库森,沙尔克04,汉堡,不莱梅,门兴格拉德巴赫,沃尔夫斯堡,里昂,马赛,巴黎圣日尔曼,里尔,摩纳哥,阿贾克斯,埃因霍温,费耶诺德,阿尔克马尔,格拉斯哥流浪者,凯尔特人,莫斯科中央陆军,圣彼得堡泽尼特,喀山鲁宾,波尔图,本菲卡,里斯本竞技,费内巴切,加拉塔萨雷,贝西克塔斯,基辅迪纳摩,顿涅茨克矿工"
  val footballstar="梅西,库蒂尼奥"
  val footballterm="足球,弧线球,鱼跃扑球,清道夫,自由人,全攻全守,下底传中,外围传中,交叉换位,长传突破,插上进攻,区域防守,补位,密集防守,造越位,反越位战术,篱笆战术,撞墙式,五大联赛,帽子戏法,乌龙球,进攻三区,防守三区,点球,球门球,角球"
  val fullterm = (footballteam + "," + footballstar + "," + footballterm).split(",")

  var fbMap = Map[String, Int]()
  for ( i <- 1 to fullterm.length ){
    fbMap.+(fullterm(i-1)->1)
  }

  def containsFB(input : String)={
    var tag = false
    for ( i <- 1 to fullterm.length )
      if ( input.contains(fullterm(i-1)) )
        tag = true
    tag
  }

  def containsFBS(input : String)={
    fullterm.map(input.contains(_)).reduce(_||_)
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

    val weibo = sc.textFile("weibo.txt").map(_.split("\t")).filter(_.length>5).filter(a=>containsFBS(a(5))).map(a=>(a(1),1)).reduceByKey(_+_).filter(_._2>5).map(a=> LikeSport(a._1,"football")).toDF
    val resDF = up.join(weibo,"uid")
    val prop = new java.util.Properties
    prop.setProperty("serverTimezone", "UTC")
    prop.setProperty("user","root")
    prop.setProperty("password","971103")
    resDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf-8", "footb", prop)

  }
}
