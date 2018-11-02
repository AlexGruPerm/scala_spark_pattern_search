import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/*
import org.apache.spark.sql.cassandra._
import spark.implicits._
*/


object otocLogg extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
}


object PattSearch extends App {
  otocLogg.log.info("BEGIN [PattSearch]")

  val spark = SparkSession.builder()
    .master("local[*]")//"spark://10.241.5.234:7077"
    .appName("PattSearch")
    .config("spark.cassandra.connection.host","10.241.5.234")
    .config("spark.submit.deployMode","client")//"cluster"
    .config("spark.shuffle.service.enabled", "false")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.driver.allowMultipleContexts","true")
    .config("spark.cassandra.input.split.size_in_mb","128")
    .config("spark.cassandra.input.fetch.size_in_rows","10000")
    .config("spark.driver.cores","1")
    .config("spark.cores.max","2")
    .config("spark.driver.memory","1g")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores","1")
    .getOrCreate()

  /*
    .master("spark://10.241.5.234:7077")
    .appName("PattSearch")
    .config("spark.cassandra.connection.host","10.241.5.234")
    .config("spark.submit.deployMode","cluster")
    .config("spark.shuffle.service.enabled", "false")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.driver.allowMultipleContexts","true")
    .config("spark.cassandra.input.split.size_in_mb","32")
    .config("spark.cassandra.input.fetch.size_in_rows","1000")
    .config("spark.driver.cores","1")
    .config("spark.cores.max","2")
    .config("spark.driver.memory","1g")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores","1")
    .getOrCreate()
  */

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
  import org.apache.spark.sql.types._

  /** UDAF, for accumulate all rows as whole instance for compare laster with pattern.
    *
    */
  class ComparePatter() extends UserDefinedAggregateFunction {

    // Input Data Type Schema of Rows.
    def inputSchema: StructType = StructType(Array(
                                                   StructField("ts_begin", IntegerType),
                                                   StructField("btype",    StringType),
                                                   StructField("disp",     DoubleType),
                                                   StructField("log_co",   DoubleType)
                                                  )
                                            )

    // Intermediate Schema
    def bufferSchema = StructType(Array(
      StructField("sum", DoubleType),
      StructField("cnt", LongType)
    ))

    // Returned Data Type .
    def dataType: DataType = DoubleType

    // Self-explaining
    def deterministic = true

    // This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer) = {
      buffer(0) = 0.toDouble // set sum to zero
      buffer(1) = 0L // set number of items to 0
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row) = {
      // With [0] - java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Double
      // Because 0 it's a ts_begin (Int)
      buffer(0) = buffer.getDouble(0) + input.getDouble(2)
      buffer(1) = buffer.getLong(1) + 1
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(2) // get field disp
      buffer1(1) = buffer1.getLong(1) + 1//buffer2.getLong(1)
    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row) = {
      buffer.getDouble(0)/buffer.getLong(1).toDouble
    }

  }




  def getBarsFromCass(TickerID :Int, BarWidthSec :Int) = {
    import org.apache.spark.sql.functions._
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "bars", "keyspace" -> "mts_bars"))
      .load()
      .where(col("ticker_id")     === TickerID &&
             col("bar_width_sec") === BarWidthSec)
      .select(col("ts_begin"), col("btype"), col("disp"), col("log_co"))
      .sort(asc("ts_begin"))
  }

  val t1_common = System.currentTimeMillis

  val listBars = getBarsFromCass(1,30)
  listBars.printSchema()

  otocLogg.log.info("listBars.count()=["+listBars.count()+"]")

  listBars.take(10) foreach{
   thisRow =>
    println(" -> "+
            thisRow.getAs("ts_begin").toString+" "+
            thisRow.getAs("btype").toString+" "+
            thisRow.getAs("disp").toString+" "+
            thisRow.getAs("log_co").toString)
  }

  //df.withColumn("colD", collect_set("colC").over(Window.partitionBy("colA").orderBy("colB"))).show(false)

  otocLogg.log.info("~ with window show ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  // Window function example
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions.col

  val windowSpec = Window
    .orderBy(col("ts_begin").asc)
    .rowsBetween(Window.currentRow, 2)

  val compPatt = new ComparePatter()
//df.groupBy("group_id").agg(expr("gm(id) as GeometricMean")).show()
  listBars.withColumn("comp_patt", compPatt(listBars.col("ts_begin"),
                                                      listBars.col("btype"),
                                                      listBars.col("disp"),
                                                      listBars.col("log_co")
                                                     ) over windowSpec).show()

  otocLogg.log.info("~ with window show ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

  /*
  listBars.createOrReplaceTempView("t_last_bars")
  otocLogg.log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  spark.sql("SELECT * FROM t_last_bars").show()
  otocLogg.log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
 */

  val t2_common = System.currentTimeMillis
  otocLogg.log.info("================== SUMMARY ========================================")
  otocLogg.log.info(" DURATION :"+ ((t2_common - t1_common)/1000.toDouble) + " sec.")
  otocLogg.log.info("================== END [OraToCass] ================================")
}
