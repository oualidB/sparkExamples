import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PopularMovie {


  def main(args: Array[String]){

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMovie")

    val lines = sc.textFile("u.data")

    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    val movieCount = movies.reduceByKey((x,y)=> x+y)

    val flipped = movieCount.map(x=>(x._2, x._1))

    //sort
    val sortedMovies =flipped.sortByKey()

    //collect and print
    val results = sortedMovies.collect()

    for(result <- results){
      val idMovie = result._2
      val viewNum = result._1
      println(s"$idMovie : $viewNum")
    }

  }

}
