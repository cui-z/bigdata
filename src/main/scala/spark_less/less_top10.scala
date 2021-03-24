package spark_less

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object less_top10 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("count_logs")
    val sc = new SparkContext(conf)

    val report_rdd = sc.textFile("./less_data")

    val acc = new HotCategoryAccumulator
    sc.register(acc, "hotcategory")

    report_rdd.foreach(
      act => {
        val datas: Array[String] = act.split("_")
        if (datas(6) != "-1") {
          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add((id, "order"))
            }
          )
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add((id, "pay"))
            }
          )
        }

      }


    )
    val accvalues: mutable.Map[String, HotCategory] = acc.value
    //    accvalues.foreach(println)

    val categories: mutable.Iterable[HotCategory] = accvalues.map(_._2)

    val sort_list: List[HotCategory] = categories.toList.sortWith(
      (a, b) => {
        if (a.click > b.click) {
          true
        } else if (a.click == b.click) {

          if (a.order > b.order) {
            true
          } else if (a.order == b.order) {
            a.pay > b.pay
          } else {
            false
          }
        } else {
          false
        }
      }
    )


    sort_list.take(10).foreach(println)

  }

  case class HotCategory(cid: String, var click: Int, var order: Int, var pay: Int)

  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val ac_type: String = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (ac_type == "click") {
        category.click += 1
      } else if (ac_type == "order") {
        category.order += 1
      } else if (ac_type == "pay") {
        category.pay += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value
      map2.foreach {
        case (id, hc) => {
          val category = map1.getOrElse(id, HotCategory(id, 0, 0, 0))
          category.pay += hc.pay
          category.order += hc.order
          category.click += hc.click
          map1.update(id, category)
        }
      }


    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }


}
