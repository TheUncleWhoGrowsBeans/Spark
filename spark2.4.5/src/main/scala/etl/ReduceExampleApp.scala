package etl

/**
 *
 * @author UncleBean
 * @date 2020-04-23 14:10
 *
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object ReduceExampleApp {

  def reduceExample(sc: SparkContext): Unit = {

    val words = sc.textFile("C:\\bd\\data\\the soul.txt").map(x => x.split(" ").length)
    val wordCount = words.reduce((x, y) => x + y)
    println(s"wordCount=$wordCount")

  }

  def reduceByKeyExample(sc: SparkContext): Unit = {

    val words = sc.textFile("C:\\bd\\data\\the soul.txt").flatMap(x => x.split(" "))
    val wordCountDS = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    println(wordCountDS)
    wordCountDS.sortBy(_._2, false).take(3).foreach(println)

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName()).setMaster("local[4]")
    val sc = new SparkContext(conf)

    reduceByKeyExample(sc)

  }

}
