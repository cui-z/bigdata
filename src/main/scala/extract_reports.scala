//import java.security.MessageDigest
//
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.util.parsing.json.JSON
//
//object extract_reports {
//  def main(args: Array[String]): Unit = {
//
//    def hashMD5(content: String): String = {
//      val md5 = MessageDigest.getInstance("MD5")
//      val encoded = md5.digest((content).getBytes)
//      encoded.map("%02x".format(_)).mkString
//    }
//    val conf = new SparkConf()
//    conf.setMaster("local").setAppName("count_logs")
//    val sc = new SparkContext(conf)
//
//    val report_rdd = sc.textFile("./reports")
//    val reports = report_rdd.map(line => JSON.parseFull(line))
//    reports.foreach(println)
////    reports.foreach(
////      {
////        r => r match {
////          case Some(map:Map[String,Any]) => {
////            val finl_str=s"${map("md5")}+${map("sha1")}+${map("sha256")}"
////            val md5_str=hashMD5(finl_str)
////            println(md5_str)
////            println(map("scans"))
//////            var unit:Map[String,Any] = map("scans")
//////            println(unit)
////          }
////          case None => println("parsing failed!")
////          case other => println("unknown data structure" + other)
////        }
////      }
////    )
//
//  }
//
//}
