package Streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object OddStringSum {

  def main(args: Array[String]): Unit = {
    def  Get_Lines_Sum(input : String) : Double = {

      val line = input.split("")
       var number : Double = 0.0
      for (x <- line)
        {
          try{
            val value = x.toDouble
            number = number + value
          }
          catch
            {
              case ex : Exception => {}
            }
        }
       return number
    }

    println("hey, Spark Streaming Session powered by Scala")

    val conf = new SparkConf().setMaster("local[2]").setAppName("EvenLines")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("Spark Context Created")

    val ssc = new StreamingContext(sc, Seconds(20))
    println("Spark Streaming Context Created ")

    val lines = ssc.socketTextStream("localhost", 9999)
    val lines_filter = lines.filter(x => Get_Lines_Sum(x)%2 == 0)
    val lines_sum = lines_filter.map(x => Get_Lines_Sum(x))

    println("Lines with even sum:")
    lines_filter.print()

    println("Sum of the numbers in even lines:")
    lines_sum.reduce(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}