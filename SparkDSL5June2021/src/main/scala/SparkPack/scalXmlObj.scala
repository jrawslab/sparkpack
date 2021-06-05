package SparkPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.databricks.spark.avro
import com.databricks.spark.xml


object scalXmlObj {
	def main (args:Array[String] ): Unit={

			val con= new SparkConf().setAppName("SparkCase").setMaster("local[*]")
					val sc= new SparkContext(con)
					sc.setLogLevel("Error")

					val spark=SparkSession.builder().appName("sql").getOrCreate()
					import spark.implicits._

					/*	val xmldf=spark.read
					                    .format("com.databricks.spark.xml")
					                    .option("rowTag","pnr")
					                    .load("file:///C:/data/xmldata.xml")
					xmldf.printSchema()
					xmldf.show() 
					 */
					val xmldf_transactions=spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("file:///C:/data/transactions.xml")
					xmldf_transactions.printSchema()
					xmldf_transactions.show(false)

					val xmldf_book=spark.read.format("com.databricks.spark.xml").option("rowTag","book").load("file:///C:/data/book.xml")
					xmldf_book.printSchema()
					xmldf_book.show()
					
					val xmldf_note=spark.read.format("com.databricks.spark.xml").option("rowTag","note").load("file:///C:/data/note.xml")
					xmldf_note.printSchema()
					xmldf_note.show()
					



	}
}