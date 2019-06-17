import java.io.{File, PrintWriter}

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.ml._

import scala.collection.mutable.ArrayBuffer

case class Relation(a: String, b: String, aTob: Double, bToa: Double)

case class People(name: String, r: Double, p: Double)

case class Pair(a: String, b: String)

object Function {

  var numAs = List[Relation]()
  var numBs = List[People]()

  private var relation = List[Relation]()
  private var people = List[People]()
  private var pairs = List[Pair]()

  def getNewRelation(i: Relation) {

    var possibility: Double = 0.0

    for (j <- numAs) {
      if (i.b == j.a) {
        val p1 = people.filter(_.name == i.a).head
        val p2 = people.filter(_.name == j.b).head
        if (!relation.exists(r => r.a == p1.name && r.b == p2.name) &&
          !relation.exists(r => r.b == p1.name && r.a == p2.name) &&
          !pairs.exists(x => x.a == p1.name && x.b == p2.name) &&
          !pairs.exists(x => x.b == p1.name && x.a == p2.name) &&
          (p1.name != p2.name)) {

          possibility = (p1.p + p2.p) / 2
          val rand = scala.util.Random.nextInt(100).toDouble
          if (possibility >= rand) {
            val r: Relation = Relation(p1.name,
              p2.name,
              p2.p * possibility / 100,
              p1.p * possibility / 100)

            relation = r :: relation
            val pair1 = Pair(p1.name, p2.name)
            val pair2 = Pair(p2.name, p1.name)
            pairs = pair1 :: pairs
            pairs = pair2 :: pairs
          }
        }
      }

      if (i.a == j.a) {
        val p1 = people.filter(_.name == i.b).head
        val p2 = people.filter(_.name == j.b).head
        if (!relation.exists(r => r.a == p1.name && r.b == p2.name) &&
          !relation.exists(r => r.b == p1.name && r.a == p2.name) &&
          !pairs.exists(x => x.a == p1.name && x.b == p2.name) &&
          !pairs.exists(x => x.b == p1.name && x.a == p2.name) &&
          (p1.name != p2.name)) {

          possibility = (p1.p + p2.p) / 2
          val rand = scala.util.Random.nextInt(100).toDouble
          if (possibility >= rand) {
            val r: Relation = Relation(p1.name,
              p2.name,
              p2.p * possibility / 100,
              p1.p * possibility / 100)

            relation = r :: relation
            val pair1 = Pair(p1.name, p2.name)
            val pair2 = Pair(p2.name, p1.name)
            pairs = pair1 :: pairs
            pairs = pair2 :: pairs
          }
        }
      }
    }
  }

  def changeRelation(r: Relation) {
    val p1 = people.filter(_.name == r.a).head
    val p2 = people.filter(_.name == r.b).head
    val jie = 0.1 * p1.r
    val huan = 0.1 * p2.r

    var change = false
    var atob = r.aTob
    var btoa = r.bToa
    var ra = p1.r
    var rb = p2.r
    val pa = p1.p
    val pb = p2.p

    if (0.3 * p2.r < jie) {
      //后者不借钱
      change = true
      atob = atob - Math.log(atob)
    } else {
      ra = ra + jie
      rb = rb - jie
      btoa = btoa * (1 - jie / p2.r)
    }
    if (0.3 * p1.r < huan) {
      //前者不还钱
      change = true
      btoa = btoa - Math.log(btoa)
      atob = atob * (1 - huan / p1.r)
    } else {
      rb = rb + huan
      ra = ra - huan
      atob = atob * (1 - huan / p1.r)
    }

    val afterR = Relation(r.a, r.b, atob, btoa)
    val aftera = People(r.a, ra, pa)
    val afterb = People(r.b, rb, pb)
    relation = relation.filter(x => judge(x.a, x.b, r.a, r.b))
    if (afterR.aTob >= 5 && afterR.bToa >= 5) {
      relation = afterR :: relation
    }
    people = people.filter(_.name != r.a).filter(_.name != r.b)
    if (ra >= 0) {
      people = aftera :: people
    } else {
      //删除跟第一个有关的关系
      relation = relation.filter(x => x.a != r.a && x.b != r.a)
    }
    if (rb >= 0) {
      people = afterb :: people
    } else {
      //删除跟第二个有关的关系
      relation = relation.filter(x => x.a != r.b && x.b != r.b)
    }
  }

  def judge(xa: String, xb: String, ra: String, rb: String): Boolean = {
    if (xa == ra && xb == rb) {
      return false
    }
    true
  }

  def main(args: Array[String]): Unit = {
    /**
      * r_three.txt:
      * [Entity1],[Entity2],[1 to 2],[2 to 1]
      *
      * p_three.txt:
      * [name],[r],[p]
      *
      *pairs.txt:
      * [Entity1],[Entity2]
      */
    val file1 = "r_three.txt"
    val file2 = "p_three.txt"
    val file3 = "pairs.txt"

    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    //    val lines = ssc.socketTextStream("localhost", 4700)
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    numAs = sc.textFile(file1)
      .map(_.split(","))
      .map(a => Relation(a(0), a(1), a(2).toDouble, a(3).toDouble))
      .collect().toList
    numBs = sc.textFile(file2)
      .map(_.split(","))
      .map(a => People(a(0), a(1).toDouble, a(2).toDouble))
      .collect().toList
    val numCs = sc.textFile(file3)
      .map(_.split(","))
      .map(a => Pair(a(0), a(1)))
      .collect().toList

    relation = numAs
    people = numBs
    pairs = numCs

    for (i <- relation) {
      val p1 = Pair(i.a, i.b)
      val p2 = Pair(i.b, i.a)
      pairs = p1 :: pairs
      pairs = p2 :: pairs
      changeRelation(i)
    }
    for (i <- relation) {
      getNewRelation(i)
    }

    relation.sortBy(r => (r.a, r.b))
      .toDF()
      .repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "false") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("15_relation.txt")

    people.sortBy(_.name)
      .toDF()
      .repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "false") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("15_people.txt")

    pairs.distinct
      .sortBy(p => (p.a, p.b))
      .toDF()
      .repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "false") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("15_pairs.txt")
  }
}
