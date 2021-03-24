import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object less_test01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("cout_ad")
    val sc = new SparkContext(conf)

    val resuRdd: RDD[((String, String), Int)] = sc.textFile("./logs/agent.log").map(line => {
      val li = line.split(" ")
      ((li(2), li(4)), 1)
    }).reduceByKey(_ + _)
    resuRdd.map({ case ((a,b),c) => {(a,(b,c))}}).groupByKey().mapValues(it => {
      it.toList.sortBy(_._2).reverse.take(3)
    }).collect().foreach(println)

  }

}
