package SparkPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.databricks.spark.avro
import com.databricks.spark.xml


object scalaRDBMSObj {
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
					
					val xmldf_rdbms=spark.read
					                            .format("jdbc")
					                            .option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/zeyodb")
					                            .option("driver","com.mysql.jdbc.Driver")
					                            .option("dbtable","web_customer")
					                            .option("user","root")
					                            .option("password","Aditya908")					                            
					                            .load()
					xmldf_rdbms.printSchema()
					xmldf_rdbms.show(1)
					
					xmldf_rdbms.write.format("parquet").mode("overwrite").save("file:///C:/data/5June2021/rdbmsparquet/")
					println("Written Parquet file")
					

			 



	}
}