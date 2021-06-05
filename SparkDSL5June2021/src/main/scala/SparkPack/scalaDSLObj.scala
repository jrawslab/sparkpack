package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.databricks.spark.avro

object scalaDSLObj {
  
  def main (args:Array[String] ): Unit={
    
			val con= new SparkConf().setAppName("SparkCase").setMaster("local[*]")
					val sc= new SparkContext(con)
					sc.setLogLevel("Error")

					val spark=SparkSession.builder().appName("sql").getOrCreate()
					import spark.implicits._
					
					val txndf= spark.read.format("csv").option("header","true").load("file:///c:/data/txns")
					txndf.show();
			    val sldf=txndf.select("*").show()
  }
  
}