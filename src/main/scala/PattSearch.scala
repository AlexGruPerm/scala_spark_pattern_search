import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

  def getBarsFromCass(TickerID :Int, BarWidthSec :Int) = {
    import org.apache.spark.sql.functions._
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "bars", "keyspace" -> "mts_bars"))
      .load()
      .where(col("ticker_id")     === TickerID &&
             col("bar_width_sec") === BarWidthSec)
      .select(col("ts_begin"), col("btype"), col("disp"), col("log_co"))
      .sort(asc("ts_begin"))
      .withColumn("btype",
         when(col("btype") === lit("g"), 1)
        .when(col("btype") === lit("r"), -1)
        .otherwise(0))
  }

  case class T_BAR(ts_begin :Long, btype :Long, disp:Double, log_co :Double){

    /** All bars in one Row.
      * Like:
      * index:     0       1       2       3          4        5      6        7          8       9      10        11
      * +----------+-------+-------+--------+----------+-------+-------+--------+----------+-------+-------+--------+
      * |1_ts_begin|1_btype| 1_disp|1_log_co|2_ts_begin|2_btype| 2_disp|2_log_co|3_ts_begin|3_btype| 3_disp|3_log_co|
      * +----------+-------+-------+--------+----------+-------+-------+--------+----------+-------+-------+--------+
      *     barIndexStartField - first column for this bar extraction, in the example: 0,4,8
      *     and each bar has 4 elements.
      */

    def this(r :Row, barIndexStartField :Int)=
      this(
        r.getLong(barIndexStartField),
        r.getLong(barIndexStartField+1),
        r.getDouble(barIndexStartField+2),
        r.getDouble(barIndexStartField+3)
      )

  }

  case class T_FORM(seqBars :Seq[T_BAR])

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions.{col, _}
  import spark.implicits._

  /*
  def compareSeqWithPattern(r :Row) = {
    val barsCount = r.size/4
    val barsForm = T_FORM( for(i <- Range(0,11,4)) yield {
      new T_BAR(r,i)
    })
    otocLogg.log.info("compareSeqWithPattern r.size="+r.size+" barsForm.size="+barsForm.seqBars.size)
    1 // barsForm
  }
  */
  //spark.udf.register("compareSeqWithPattern", compareSeqWithPattern _)

  def udf_comp(p: Row) = udf(
    (r: Row) =>
    {
      val barsCount = r.size / 4
      val barsForm = T_FORM(for (i <- Range(0, 11, 4)) yield {
        new T_BAR(r, i)
      })
      val barPattern = T_FORM(Seq(new T_BAR(p, 0)))
      otocLogg.log.info("[udf_comp] r.size=" + r.size + " barsForm.size=" + barsForm.seqBars.size + " barPattern.seqBars.size=" + barPattern.seqBars.size)
      if ((barPattern.seqBars(0).btype == barsForm.seqBars(0).btype) && (barPattern.seqBars(0).disp == barsForm.seqBars(0).disp))
        1
      else
        0
    }
  )

  /**
    *
    * @param compPattern - Dataframe with exact one row - pattern for search
    * @param df - history Dataframe where we search comparison
    * @return df with comparison result column
    */
  def ctCompareFormWithPattern(compPattern: DataFrame)(df: DataFrame): DataFrame = {
    //ctCompareFormWithPattern compPattern.count=1 df.count=943
    otocLogg.log.info(">>>>>>>     ctCompareFormWithPattern compPattern.count="+compPattern.count()+" df.count="+df.count())
    df.withColumn("compare_result",udf_comp(compPattern.first())(struct(df.columns.map(df(_)) : _*)) )
  }

  val t1_common = System.currentTimeMillis
  val listBars = getBarsFromCass(1,300)

  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
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
  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/



  val nBuckets = 3

  val lb = listBars
    .withColumn("rn", row_number() over Window.orderBy(col("ts_begin").asc) )
    .withColumn("rn", floor(($"rn"-1)/nBuckets))
    .withColumn("rnk", row_number() over Window.partitionBy("rn").orderBy(col("ts_begin").asc) )
    //-------------------------
    .groupBy("rn")
    .pivot("rnk", 1 to nBuckets)
    .agg(
      sum("ts_begin").alias("ts_begin"),
      sum("btype").alias("btype"),
      sum("disp").alias("disp"),
      sum("log_co").alias("log_co")
    )
    //-------------------------

    //For example this is a first row.
    val barsPattern = lb.filter($"rn" === 0).select("1_ts_begin", "1_btype", "1_disp", "1_log_co")
    //barsPattern.printSchema()
    val newNames = Seq("0_ts_begin", "0_btype", "0_disp", "0_log_co")
    val bpRowDf = barsPattern.toDF(newNames: _*)
   /**
     * +----------+-------+-------+--------+
     * |0_ts_begin|0_btype| 0_disp|0_log_co|
     * +----------+-------+-------+--------+
     */
    // bpRowDf.printSchema()
    // bpRowDf.show()

    otocLogg.log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    lb.drop("rn").transform(ctCompareFormWithPattern(bpRowDf)).show()
    otocLogg.log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    /*
   val lbPrepJoined = lb.crossJoin(bpRowDf)

   lbPrepJoined.printSchema()
   lbPrepJoined.show


  val lbPrepared = lb.withColumn("compare_result",callUDF("compareSeqWithPattern",struct(lb.drop("rn").columns.map(lb(_)) : _*)  ))
  lbPrepared.show
  */

  val t2_common = System.currentTimeMillis
  otocLogg.log.info("================== SUMMARY ========================================")
  otocLogg.log.info(" DURATION :"+ ((t2_common - t1_common)/1000.toDouble) + " sec.")
  otocLogg.log.info("================== END [OraToCass] ================================")
}
