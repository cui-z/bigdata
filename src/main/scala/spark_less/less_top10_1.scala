package spark_less

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object less_top10_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("count_logs")
    val sc = new SparkContext(conf)

    val report_rdd: RDD[String] = sc.textFile("hdfs:///home/cuizhe1/user_visit_action.txt")
    val union_results: RDD[(String, (Int, Int, Int))] = report_rdd.flatMap(line => {
      val split_list = line.split("_")
      if (split_list(6) != "-1") {
        List((split_list(6), (1, 0, 0)))
      } else if (split_list(8) != "null") {
        val ids = split_list(8).split(",")

        ids.map(id => (id, (0, 1, 0)))
      } else if (split_list(10) != "null") {
        val ids = split_list(10).split(",")
        ids.map(id => {
          (id, (0, 0, 1))})
      } else {
        Nil
      }
    })

//    union_results.foreach(x => {
//      println(x.getClass)
//    })
//    union_results.foreach(println)
    val results = union_results.reduceByKey { case ((a, b, c), (d, e, f)) => {
      (a + d, b + e, c + f)
    }
    }
    val final_results = results.sortBy(_._2._2,false).take(5)
    final_results.foreach(println)



  }

}
