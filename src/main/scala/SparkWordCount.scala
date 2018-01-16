import org.apache.spark.sql.SparkSession

object SparkWordCount {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    //val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
   val spark  = SparkSession
      .builder()
      .appName("单词计数")
      .getOrCreate()

    val sc = spark.sparkContext
        sc.setJobGroup("zhang-test","测试Job分组")
    // get threshold
    val threshold = args(1).toInt

    // read in text file and split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    println(wordCounts)

    // filter out words with fewer than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    System.out.println(charCounts.collect().mkString(", "))

//    sc.cancelJobGroup("zhang-test")
  }
}