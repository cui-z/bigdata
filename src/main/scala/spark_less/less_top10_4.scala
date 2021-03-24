package spark_less

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//Top10热门品类中每个品类的 Top10 活跃 Session 统计
object less_top10_4 {
  case class user_vissit_act(date: String,//用户点击行为的日期
                             user_id: Long,//用户的ID
                             session_id: String,//Session的ID
                             page_id: Long,//某个页面的ID
                             action_time: String,//动作的时间点
                             search_keyword: String,//用户搜索的关键词
                             click_category_id: Long,//某一个商品品类的ID
                             click_product_id: Long,//某一个商品的ID
                             order_category_ids: String,//一次订单中所有品类的ID集合
                             order_product_ids: String,//一次订单中所有商品的ID集合
                             pay_category_ids: String,//一次支付中所有品类的ID集合
                             pay_product_ids: String,//一次支付中所有商品的ID集合
                             city_id: Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("count_logs")
    val sc = new SparkContext(conf)

    //val report_rdd: RDD[String] = sc.textFile("file:///software/spark/bin/tmp_data/user_visit_action.txt")

    val args = sc.getConf.get("spark.driver.args")

    println(args)
    println(args(0))
    val report_rdd: RDD[String] = sc.textFile(args)
    val actionDataRDD: RDD[user_vissit_act] = report_rdd.map(
      line => {
        val datas = line.split("_")
        user_vissit_act(datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong)
      }
    )
    actionDataRDD.cache()

    val ids: List[Long] = List[Long](1,2,3,4,5,6,7)
    //    ids.zip()
    //    println(ids.tail)
    val okflowids: List[(Long, Long)] = ids.zip(ids.tail)
    //okflowids.foreach(println)
    val pageridcountmap: Map[Long, Long] = actionDataRDD.filter(
      ac => {
        ids.init.contains(ac.page_id)
      }
    ).map(
      ac => {
        (ac.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap
    //pageridcountmap.foreach(println)

    val sessionrdd: RDD[(String, Iterable[user_vissit_act])] = actionDataRDD.groupBy(_.session_id)
    //sessionrdd.foreach(println)
    val mvrdd: RDD[(String, List[((Long, Long), Int)])] = sessionrdd.mapValues(
      iter => {
        val sortlist: List[user_vissit_act] = iter.toList.sortBy(_.action_time)
        val flowlist = sortlist.map(_.page_id)

        val pageflowIds: List[(Long, Long)] = flowlist.zip(flowlist.tail)

        pageflowIds.filter(
          t => okflowids.contains(t)
        ).map(t => {
          (t, 1)
        })
      }
    )
    val flatrdd: RDD[((Long, Long), Int)] = mvrdd.map(_._2).flatMap(a => a)
    val datardd: RDD[((Long, Long), Int)] = flatrdd.reduceByKey(_+_)
    //mvrdd
    //datardd.foreach(println)

    datardd.foreach{
      case ((a,b),c) => {
        val dow: Long = pageridcountmap.getOrElse(a,0L)
        println(c)
        println(dow)
        println(s"页面${a}跳转到页面${b}的转换率为："+(c.toDouble/dow))

      }
    }

  }

}
