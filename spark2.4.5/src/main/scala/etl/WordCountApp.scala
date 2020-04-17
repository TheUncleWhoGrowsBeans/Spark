package etl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * @author UncleBean
 * @date 2020-04-17 18:25
 *
 */


object WordCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val distFile = sc.textFile("C:\\bd\\data\\the soul.txt")
    println(distFile)

    distFile.flatMap(
      s => s.split(" |,|;|'s")
    ).filter(
      x => x.length() >= 1
    ).map(
      x => (x, 1)
    ).reduceByKey(
      (x, y) => x + y
    ).sortBy(
      _._2,
      false
    ).collect().foreach(
      x => println(s"${x._1}: ${x._2}")
    )

  }
}
