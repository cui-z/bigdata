package spark_less

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//Top10热门品类中每个品类的 Top10 活跃 Session 统计
object less_top10_3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("count_logs")
    val sc = new SparkContext(conf)

    val report_rdd: RDD[String] = sc.textFile("file:////root/spark-3.0.0-bin-hadoop3.2/bin/tmp_data/user_visit_action.txt")
    //val report_rdd: RDD[String] = sc.textFile("hdfs://localhost:9000/cuizhe1/user_visit_action.txt")
    report_rdd.cache()



    def top10Category(actionRDD:RDD[String]) = {
      val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
        action => {
          val datas = action.split("_")
          if (datas(6) != "-1") {
            // 点击的场合
            List((datas(6), (1, 0, 0)))
          } else if (datas(8) != "null") {
            // 下单的场合
            val ids = datas(8).split(",")
            ids.map(id => (id, (0, 1, 0)))
          } else if (datas(10) != "null") {
            // 支付的场合
            val ids = datas(10).split(",")
            ids.map(id => (id, (0, 0, 1)))
          } else {
            Nil
          }
        }
      )

      val analysisRDD = flatRDD.reduceByKey(
        (t1, t2) => {
          ( t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3 )
        }
      )

      analysisRDD.sortBy(_._2, false).take(10).map(_._1)
    }
    val top10_results: Array[String] = top10Category(report_rdd)
    //top10_results.foreach(println)
    val filte_results = report_rdd.filter(
      line => {
        if (line.split("_")(6) != "-1") {
          top10_results.contains(line.split("_")(6))
        } else {
          false
        }
      }
    )
    val count_resu = filte_results.map(line => {
      val split_lines = line.split("_")
      ((split_lines(6), split_lines(2)), 1)
    }).reduceByKey(_ + _)
   // count_resu.foreach(println)

    val maprdd: RDD[(String, (String, Int))] = count_resu.map {
      case ((a, b), c) => {
        (a, (b, c))
      }
    }
    val groupresu: RDD[(String, Iterable[(String, Int)])] = maprdd.groupByKey()

    val results: RDD[(String, List[(String, Int)])] = groupresu.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(5)
      }
    )
    results.foreach(println)



  }

}
