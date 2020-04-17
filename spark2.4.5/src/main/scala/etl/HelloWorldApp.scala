package etl

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object HelloWorldApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName()).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data, 2)
    distData.foreach(println)
    println(distData)

    val result = distData.reduce((x, y) => x + y)
    println(s"sum=$result")

  }

}