package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = TimeUsage;

  //classifiedColumns
  test("shouldClassifyPrimaryNeeds") {
    //GIVEN
    val columns = List("t500103", "t01", "t03", "t11", "t999", "t1801", "t1803999", "t05", "t1805")

    //WHEN
    val (primaryColumns, workColumns, otherColumns) = testObject.classifiedColumns(columns)

    //THEN
    println(primaryColumns)
    assert(primaryColumns.size == 5)
    assert(otherColumns.size == 2)
    assert(workColumns.size == 2)
  }
}
