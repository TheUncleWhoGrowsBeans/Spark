package etl

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel


object HelloWorldApp {

  def hello(sc: SparkContext): Unit = {
    var counter = 0
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data, 1)
    distData.foreach(x => counter += x)
    println(distData)

    val result = distData.reduce((x, y) => x + y)
    println(s"sum=$result counter=$counter")

  }

  def toSequenceFile(sc: SparkContext): Unit = {
    val data = sc.textFile("C:\\bd\\data\\the soul.txt", 1).persist(StorageLevel.MEMORY_ONLY)
    val wordCountDS = data.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(
      (a, b) => a + b
    ).sortBy(_._2, ascending = false)

    wordCountDS.saveAsSequenceFile("C:\\bd\\data\\the_soul_count")
  }

  def count(sc: SparkContext): Unit = {
    val data = sc.textFile("C:\\bd\\data\\the soul.txt", 2)
    val wordDS = data.flatMap(x => x.split(" ")).map(x => (x, 1))
    val wordDSreduce = wordDS.reduceByKey((a, b) => a + b)
    println(wordDSreduce)
    val wordDScount = wordDS.countByKey()
    println(wordDScount)

  }

  def aggregate(sc: SparkContext): Unit = {
    val data = sc.textFile("C:\\bd\\data\\the soul.txt", 3).flatMap(x => x.split(" "))
    val result = data.map(x => (x, 1)).aggregateByKey(0, 4)(
      (a: Int, b: Int) => a + b,
      (a: Int, b: Int) => a + b
    )
    result.saveAsTextFile("C:\\bd\\data\\the_soul_count")

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName()).setMaster("local[4]")
    val sc = new SparkContext(conf)

    aggregate(sc)

  }

}