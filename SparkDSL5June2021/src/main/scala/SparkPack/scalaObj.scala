package SparkPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.databricks.spark.avro

object scalaObj {
	def main (args:Array[String] ): Unit={
			val con= new SparkConf().setAppName("SparkCase").setMaster("local[*]")
					val sc= new SparkContext(con)
					sc.setLogLevel("Error")

					val spark=SparkSession.builder().appName("sql").getOrCreate()
					import spark.implicits._
					/*
				  val src = args(0)
					val dest = args(1)
					val mode1=args(2)
					val writeformat=args(3)
					val sourceformat=args(4)

					val srcMGS="==================Read " +sourceformat +" file ==================================="
					val destMGS="=================Written " +writeformat+ " file =================================="			


					val usdata = spark.read.format(sourceformat).option("header", "true").load(src)    
					usdata.show()
					println()
					println(srcMGS)

					usdata.write.format(writeformat).partitionBy("category","spendby").mode(mode1).save(dest)

					println()
					println(destMGS)	
					 * 
					 */




	}

}