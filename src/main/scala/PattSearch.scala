
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

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
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.driver.allowMultipleContexts","true")
    .config("spark.cassandra.input.split.size_in_mb","128")
    .config("spark.cassandra.input.fetch.size_in_rows","10000")
    .config("spark.driver.cores","2")
    .config("spark.cores.max","4")
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory", "3g")
    .config("spark.executor.cores","2")
    .getOrCreate()

  /*
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
  */

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
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions.{col, _}
  import spark.implicits._
  */

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
       if ((barPattern.seqBars(0).btype == barsForm.seqBars(0).btype) &&
            (
              (barPattern.seqBars(0).disp >= barsForm.seqBars(0).disp*0.8) &&
              (barPattern.seqBars(0).disp <= barsForm.seqBars(0).disp*1.2)
            )
          )
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



  import org.apache.spark.sql.types._

  class ComparePatter() extends UserDefinedAggregateFunction {

    // Input Data Type Schema of Rows.
    def inputSchema: StructType = StructType(Array(
      StructField("comp_pattern_rn", ArrayType(IntegerType))
    )
    )

    // Intermediate Schema
    def bufferSchema: StructType = {
      StructType(
            StructField("row_rn_window", IntegerType) :: //index of row : 1,2,3 colculated inside window.
            StructField("inp_array",     ArrayType(IntegerType)) ::
            StructField("cnt_eq",        IntegerType) :: Nil
      )
    }


    // Returned Data Type .
    def dataType: DataType = IntegerType //        ArrayType(IntegerType)        //IntegerType


    // Self-explaining
    def deterministic = true

    // This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer) = {
        buffer(0) = 0    // row_rn_window, 1 for first row.
        buffer(1) = Nil  // inp_array
        buffer(2) = 0    // cnt_eq
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row) = {

      buffer(1) = input.getAs[mutable.WrappedArray[Int]](0) //One field array []

      val wpA = buffer.getAs[mutable.WrappedArray[Int]](1)

      //otocLogg.log.info("======== >>>>>>>>>  " + buffer(1) + "   for take by index  INDEX == buffer(0)=" + buffer(0) + "  buffer(1)(0)= " +wpA(0) + "  buffer(1)(1)= "+wpA(1)+ "  buffer(1)(2)= "+wpA(2))

      val idx :Int = buffer.getInt(0)

      if (wpA(buffer.getInt(0)) == (buffer.getInt(0)+1))
        buffer(2) = buffer.getInt(2) + 1


      buffer(0) = buffer.getInt(0)+1

    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row) = {
      /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
      // Is it visible spark.sql temp table here
      //Too slow!!!
      //val tPatternDf = spark.sql(" SELECT d.* FROM t_barsPattern d ")
      //otocLogg.log.info("!!! - [evaluate] tPatternDf.count() = ["+ tPatternDf.count() +"]")
      /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
      //buffer.getDouble(0)/buffer.getLong(1).toDouble
      //1
      //buffer.getAs[ArrayType](4)
      //otocLogg.log.info("======== >>>>>>>>>  size input array [" + buffer.getAs[mutable.WrappedArray[Int]](1).size+" ]")
      //otocLogg.log.info("======== >>> FOR TEST cnt_eq [" + buffer.getInt(2) + " ]")
      //buffer.getInt(0)
      if (buffer.getAs[mutable.WrappedArray[Int]](1).size==buffer.getInt(2))
        1
      else 0
      //buffer.getAs[mutable.WrappedArray[Int]](1)
    }

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

  val windowSpec = Window
    .orderBy(col("ts_begin").asc)
    .rowsBetween(Window.currentRow, nBuckets-1)


  //get 3 first (order by ts_begin asc) rows as pattern for search
  val barsPattern = listBars
    .orderBy($"ts_begin".asc)
    .limit(nBuckets)
    .withColumn("rn", row_number() over Window.orderBy(col("ts_begin").asc))


  otocLogg.log.info("barsPattern last")
  barsPattern.show()

  otocLogg.log.info("listBars last")

  listBars.createOrReplaceTempView("t_listBars")

  barsPattern.persist()
  broadcast(barsPattern)
  barsPattern.createOrReplaceTempView("t_barsPattern")

  val compPatt = new ComparePatter()
  spark.udf.register("compPatt", compPatt)

  val joinedHistPattern = spark.sql(""" SELECT
                                                       ds.*
                                                          ,array(
                                                                 COALESCE(d1.rn,0),
                                                                 COALESCE(d2.rn,0),
                                                                 COALESCE(d3.rn,0)
                                                                ) as comp_pattern_rn,
                                                       compPatt(
                                                            array(
                                                                  COALESCE(d1.rn,0),
                                                                  COALESCE(d2.rn,0),
                                                                  COALESCE(d3.rn,0)
                                                                 )
                                                       )                       OVER (ORDER BY ds.ts_begin asc ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) as res_eq_pattern,
                                                       LAST_VALUE(ds.ts_begin) OVER (ORDER BY ds.ts_begin asc ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) as search_form_last_bar
                                       FROM  t_listBars ds
                                        LEFT JOIN t_barsPattern d1 ON ds.btype = d1.btype and d1.disp BETWEEN ds.disp*0.7 and ds.disp*1.3 and d1.rn=1
                                        LEFT JOIN t_barsPattern d2 ON ds.btype = d2.btype and d2.disp BETWEEN ds.disp*0.7 and ds.disp*1.3 and d2.rn=2
                                        LEFT JOIN t_barsPattern d3 ON ds.btype = d3.btype and d3.disp BETWEEN ds.disp*0.7 and ds.disp*1.3 and d3.rn=3
                                    """)

  //joinedHistPattern.printSchema()


  joinedHistPattern.show(20)


  //joinedHistPattern.filter($"res_eq_pattern" === 1).show(20)

  val t2_common = System.currentTimeMillis
  otocLogg.log.info("================== SUMMARY ========================================")
  otocLogg.log.info(" DURATION :"+ ((t2_common - t1_common)/1000.toDouble) + " sec.")
  otocLogg.log.info("================== END [OraToCass] ================================")
}
