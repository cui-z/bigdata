import org.apache.spark.{SparkConf, SparkContext}

object less_001 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("lesson").setMaster("local")
    val sc = new SparkContext(conf)
    val dataRDD = sc.makeRDD(List(1,2,3,4),1)
    val dataRDD1 = dataRDD.groupBy(
      _%2
    )
    dataRDD1.collect().foreach(println)


  }

}
