import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.graphx._

case class Predict(uid: String, score: Int)

object sparkTest {
  val shortGood: String = "还行,不错,好评,可以的,很好,服务好"
  val goodComment: String = "性价比高,物美价廉,位置很好,服务好,服务很好,干净整洁,干净卫生,位置繁华," +
    "房间好,房间宽敞,房间大,房间够大,房间干净,房间很大,位置比较好,好评,很棒,挺好," + //通用评价
    "非常亲民,很亲民,不错,还算满意,很满意,服务赞,价格很靠谱,价格实惠,划算,热情," +
    "还可以,可以的,环境也可以,住着比较舒适,床舒服," +
    "小姐姐,良心,电影,表扬,独立,老板娘,应有尽有,飘窗,好玩,礼物,海伦,大气,很值,个星,商业,人性,站路,生好,很合"
  //这行为用户个人行为分析
  val veryGoodCmt: String = "人超,嘿嘿,非常棒,特别喜欢,超高,超赞,很美好,赞赞"
  //这排评论+2
  val badComment: String = "价格贵,不太值,难受,不舒服,不满意,有点差,不怎样,不怎么,不好,好麻烦,差评,一般吧,一般般,陈旧,淋浴,漏水,蟑螂,蚊子,坏的,有问题,少个,不太方便," +
    "很差,房间小,房间很小,房间狭隘,是松的,偏僻,位置差,服务差,网络差,有待改善," + //通用评价
    "黑斑,电线外露,声音太大,太吵,隔音不好,不隔音,隔音很一般,半夜一直很响,说话都听得见,隔音差," +
    "卫生差,房间没打扫,不干净,被子很脏,全身发痒,对得起,总是,太难,密码,高架,花洒,综合,排水,无窗,刷牙,水果,透气,较差,水有," + //卫生原因
    "环境一般,设施简陋,位置不便,不太好找,空调不行" +
    "一点也不重视,停车不方便,没有停车场,看不起,体验如何？,电视机打不开,很多烟头,门面太小,不值这个价钱,不是很舒服,有灰,服务态度差,态度很不耐烦,态度冷淡," +
    "自己套被套,空调热气不行,地板翘,卫生间没门,不能入住,被图欺骗了,掉色" //个人特殊原因
  val veryBadCmt: String = "撇,有异味,狭窄,怪味,一清二楚,彻底,外机,明明,很脏,呵呵,腰酸背痛,虫子,不堪入目,太差," +
    "一点都不属实,真的太垃圾了,特别吵,特别差,非常差,最差,再也不来了,第一次给差评,皮肤过敏,气不气" //这排评论-2

  val fiveCmt: String = "设备齐全,有情调,来接我,老板很好,这么便宜,非常感谢,非常好,特别好,物美价廉,位置很好,服务好,服务很好," +
    "非常适合,强烈,好住,价比高,选了,大窗,暖心,帮助,一路,正规,美女,高大,还会来,还来,超高,首选,舒心,五星,超好,人性化,感谢,最高,成都旅游,亲切,大赞,一流,一家人,电影院,双流,妹妹," +
    "服务到位,服务满意,服务赞,价格很靠谱,推荐,温馨,优雅,便利,实惠,热情,繁华,浪漫,特别喜欢,超高,超赞,超值,很美好,很舒服,漂亮,很高兴,很喜欢,很棒,太棒,强烈推荐" //5分大部分兄弟的评论
  val fourCmt: String = "高分,习惯了,习惯住,比较高兴,价格适中,美中不足,学校,这段话,价格公道,汽车站,省事,工号,离开,小巷,段距离,和蔼" //4分大部分兄弟的评论
  val threeCmt: String = "还好,拥挤,声音太大,网差" //3分大部分兄弟的评论
  val twoCmt: String = "被子很脏,很多烟头" //2分大部分兄弟的评论
  val oneCmt: String = "不退,承认,太坑,投诉,恶劣,再也不会,退款,房间脏,非常差,极差,最差,骗,离谱,差评,服务差,冷漠,再也,第一次给差评,太垃圾了,不堪入目" //1分大部分兄弟的评论

  val short1: Array[String] = shortGood.split(",")
  val good: Array[String] = goodComment.split(",")
  val veryGood: Array[String] = veryGoodCmt.split(",")
  val bad: Array[String] = badComment.split(",")
  val veryBad: Array[String] = veryBadCmt.split(",")

  val five: Array[String] = fiveCmt.split(",")
  val four: Array[String] = fourCmt.split(",")
  val three: Array[String] = threeCmt.split(",")
  val two: Array[String] = twoCmt.split(",")
  val one: Array[String] = oneCmt.split(",")


  def getPoint(input: String, baseRank: Int): Int = {
    val rank = baseRank

    for (i <- two.indices) {
      if (input.contains(two(i))) {
        return 2
      }
    }
    for (i <- four.indices) {
      if (input.contains(four(i))) {
        return 4
      }
    }
    for (i <- one.indices) {
      if (input.contains(one(i))) {
        return 1
      }
    }
    for (i <- five.indices) {
      if (input.contains(five(i))) {
        return 5
      }
    }
    for (i <- three.indices) {
      if (input.contains(three(i))) {
        return 3
      }
    }


    if (input.length < 6) {
      for (i <- short1.indices) {
        if (input.contains(short1(i))) {
          return 5
        }
      }
      return rank
    }
    scoreGet(input, rank) //return 这个函数的值
  }

  def scoreGet(input: String, rank: Int): Int = {
    var score = rank
    for (i <- veryGood.indices) {
      if (input.contains(veryGood(i))) {
        score = score + 2
      }
    }
    for (i <- good.indices) {
      if (input.contains(good(i))) {
        score = score + 1
      }

    }
    for (i <- bad.indices) {
      if (input.contains(bad(i))) {
        if (score >= 5) {
          score = 5
        }
        score = score - 1
      }
    }
    for (i <- veryBad.indices) {
      if (input.contains(veryBad(i))) {
        //        if (score>=5){
        //          score=5
        //        }
        score = score - 2
      }
    }

    if (score >= 5) {
      5
    }
    else if (score <= 1) {
      1
    }
    else {
      score
    }
  }

  def getFault(_2: Int, _3: Int): Boolean = {
    if (_2 == _3) {
      false
    } else {
      true
    }
  }

  def main(args: Array[String]): Unit = {
    val loadfile = "test.csv"
    val baseRank = 4

    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    //    val lines = ssc.socketTextStream("localhost", 4700)
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //val a = "a b c"
    //map拆分
    val numAs = sc.textFile(loadfile)
      .map(_.split(","))
      .filter(_.size > 1)
      .map(a => (a(0), getPoint(a(1), baseRank)))
      //.filter(a => getFault(a._2, a._3))
      .map(a => Predict(a._1, a._2))
      .toDF()

    //输出到.csv
        val outpath="output\\Get"
        numAs
          .repartition(1).write.format("com.databricks.spark.csv")
          .option("header", "false")//在csv第一行有属性"true"，没有就是"false"
          .option("delimiter",",")//默认以","分割
          .save(outpath)

    val num = numAs
      .count()
      .toDouble

    val total = sc.textFile(loadfile)
      .map(_.split(","))
      .count()
      .toDouble

    numAs.show()
    println("出错率：" + num / total)
  }
}
