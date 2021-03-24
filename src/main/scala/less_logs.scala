import org.apache.spark.{SparkConf, SparkContext}

object less_logs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("count_logs")
    val sc = new SparkContext(conf)

    val ori_rdd = sc.textFile("./data")
    ori_rdd.filter(_.contains("female")).map(line =>{
      val spdata = line.split(",")
      (spdata(0),spdata(2).toInt)
    }).reduceByKey(_+_).filter(_._2>120).collect().foreach(println)
  }

}
