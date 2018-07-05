package main

import org.apache.spark.{SparkConf, SparkContext}

object MaxRepeat {

  def main(args:Array[String]) :Unit = {

    val conf = new SparkConf ()
    conf.setMaster ("local")
    conf.setAppName ("Word Count")
    val sc = new SparkContext (conf)

    val start_time = System.nanoTime ();
    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val rdd = sc.textFile ("/home/jwala/Documents/spark/big")


    var rdd_temp = rdd.filter(_.length() !=0).flatMap (line => line.split (" ") ).map (line => (line, 1) ).reduceByKey (_+ _)
    var max_final = rdd_temp.values.max
  println(max_final)
    def filter_this(x: (String, Int)): Boolean = {

      if (x._2 == max_final)
        return true

      return false
    }
    var rdd_final = rdd_temp.filter (filter_this)
    rdd_final.collect.foreach(println)
  }

}
