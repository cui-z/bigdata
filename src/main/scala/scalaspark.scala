import org.apache.spark.{SparkConf, SparkContext}

object scalaspark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("woc").setMaster("local")
    val sc = new SparkContext(conf)
    val results = sc.textFile("./logs/file").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    results.foreach(println)

  }

}
