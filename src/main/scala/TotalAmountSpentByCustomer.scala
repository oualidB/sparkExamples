import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountSpentByCustomer {

  def amountParser ( line:String )={
    val fields = line.split(",")
    val customer = fields(0)
    val price = fields(2)
   (customer.toInt, price.toFloat)
  }

  def main(args: Array[String]){

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalAmountSpentByCustomer")

    val lines = sc.textFile("customer-orders.csv")

    val parsedLines = lines.map(amountParser)

    val amountsCount = parsedLines.mapValues(x => (x,1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    val amountsCountSorted = amountsCount.map(x=> (x._2, x._1)).sortByKey()

    val results = amountsCountSorted.collect()

    for((result1, result2) <- results){
      val customer = result2
      val amount = result1._1
      println(s"$customer:$amount")
    }
  }

}
