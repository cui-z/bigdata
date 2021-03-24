package spark_less

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object less_top10_0 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("count_logs")
    val sc = new SparkContext(conf)

    val report_rdd: RDD[String] = sc.textFile("/software/spark/bin/tmp_data/user_visit_action.txt")
    report_rdd.cache()



    val click_data = report_rdd.filter(data =>{
       data.split("_")(6) != "-1"
    })
    val click_result: RDD[(String, Int)] = click_data.map(line => {
      val list_line = line.split("_")
      (list_line(6), 1)
    }).reduceByKey(_ + _)







    val order_data = report_rdd.filter(data =>{
      data.split("_")(8) != "null"
    })
    val order_results: RDD[(String, Int)] = order_data.flatMap(line => {
      val list_line = line.split("_")(8)
      val resu_list = list_line.split(",")
      resu_list.map(x => (x, 1))
    }).reduceByKey(_ + _)



    val buy_data = report_rdd.filter(data =>{
      data.split("_")(10) != "null"
    })
    val buy_results: RDD[(String, Int)] = buy_data.flatMap(line => {
      val list_line = line.split("_")(10)
      val resu_list = list_line.split(",")
      resu_list.map(x => (x, 1))
    }).reduceByKey(_ + _)



//    val clickActionRDD = report_rdd.filter(
//      action => {
//        val datas = action.split("_")
//        datas(6) != "-1"
//      }
//    )
//    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
//      action => {
//        val datas = action.split("_")
//        (datas(6), 1)
//      }
//    ).reduceByKey(_ + _)


    val clink = click_result.map {
      case (k , v) => {
      (k, (v, 0, 0))
    }
    }
    val order = order_results.map { case (k, v) => {
      (k, (0, v, 0))
    }
    }
    val buy = buy_results.map { case (k, v) => {
      (k, (0, 0, v))
    }
    }
    val union_results: RDD[(String, (Int, Int, Int))] = clink.union(order).union(buy)
//    union_results.foreach(println)
    val results = union_results.reduceByKey { case ((a, b, c), (d, e, f)) => {
      (a + d, b + e, c + f)
    }
    }
    val final_results = results.sortBy(_._2._2,false).take(5)
    final_results.foreach(println)



  }

}
