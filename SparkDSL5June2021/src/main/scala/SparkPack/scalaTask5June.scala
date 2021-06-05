package SparkPack
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._


object scalaTask5June {
	def main (args:Array[String] ): Unit={
			val con= new SparkConf().setAppName("SparkCase").setMaster("local[*]")
					val sc= new SparkContext(con)
					sc.setLogLevel("Error")

					val spark=SparkSession.builder().appName("sql").getOrCreate()
					import spark.implicits._

					//	Task 2 - Read txns and Write the filtered data as parquet


					val txndf = spark.read.format("csv").option("header","true").load("file:///C://data//txns")
					txndf.show()
					val sel_col = txndf.select("txnno","category","product")
					sel_col.show()
					val fil_data = txndf.filter(col("category")==="Gymnastics")
					fil_data.show()

					fil_data.write.format("parquet").mode("overwrite").save("file:///c:/data/5June2021/dsl")
					println("Write Txn  file" )		




					/*   			// task 3 --Write the dataframe to the same rdbms
        					println("Read file" )		

        					val txndf= spark.read.format("csv").option("header","true").load("file:///c:/data/txns")
        					txndf.show();
        			txndf.write 
        			.format("jdbc")
        			.option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
        			.option("driver","com.mysql.jdbc.Driver")
        			.option("dbtable","jrathore_tab")
        			.option("user","root")
        			.option("password","Aditya908")	

        			.save()


        			println("Write data at RDBMS") */

					/*	// tASK 4 -----  Write with partition of category and Spendby

			println("Read Txn file" )		
			val txnpartdf= spark.read.format("csv").option("header","true").load("file:///c:/data/txns")
			txnpartdf.show();			    
			txnpartdf.write.format("parquet").partitionBy("category","spendby").mode("overwrite").save("file:///c:/data/5June2021/savepartion")
			println("Write Txn with partition file" )		
					 */



	}
}