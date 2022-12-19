package sparkSql_Hive;

//$example on:spark_hive$

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.sql.SparkSession;

public class JavaSparkHiveExample {

	public static void main(String[] args) {

		SparkSession spark = SparkSession

				.builder()
				.master("local[*]")

				.appName("Java Spark Hive Example")
				.config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
				.config("hive.metastore.uris", "thrift://localhost:9083")
				.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
				.config("hive.exec.scratchdir",
						"/tmp/a-folder-that-the-current-user-has-permission-to-write-in")
				.config("spark.yarn.security.credentials.hive.enabled", "true")
				.config("spark.sql.hive.metastore.jars", "maven")
				.config("spark.sql.hive.metastore.version", "1.2.1")
				.config("spark.sql.catalogImplementation", "hive")

				.enableHiveSupport()
				.getOrCreate();


		spark.sql("DROP TABLE IF EXISTS LivingCost");
		spark.sql("CREATE EXTERNAL TABLE LivingCost(id STRING, city STRING, "
				+ "country STRING, cheapMeal Double,medMeal DOUBLE,macMeal Double, "
				+ "beer Double, cappuccino DOUBLE,pepsi Double) "
				+ "LOCATION 'hdfs://localhost:8020/user/cloudera/input' "
				+ "ROW FORMAT DELIMITED " + "FIELDS TERMINATED BY ','");
		
		spark.sql("SELECT * FROM LivingCost LIMIT 10").show();
		
		/*
		 	+---+---------+-----------+---------+-------+-------+----+----------+-----+
			| id|     city|    country|cheapMeal|medMeal|macMeal|beer|cappuccino|pepsi|
			+---+---------+-----------+---------+-------+-------+----+----------+-----+
			|  0|    Delhi|      India|      4.9|  22.04|   4.28|1.84|      1.78| 0.48|
			|  1| Shanghai|      China|     5.59|  40.51|   5.59|1.12|      3.96| 0.52|
			|  2|  Jakarta|  Indonesia|     2.54|  22.25|    3.5|2.02|      2.19| 0.59|
			|  3|   Manila|Philippines|     3.54|   27.4|   3.54|1.24|      2.91| 0.93|
			|  4|    Seoul|South Korea|     7.16|  52.77|   6.03|3.02|      3.86| 1.46|
			|  5|  Bangkok|   Thailand|      2.6|  28.09|   5.62|2.25|      2.06|  0.5|
			|  6|  Kolkata|      India|      2.0|  14.69|   3.67| 2.2|       1.6| 0.36|
			|  7|Guangzhou|      China|     4.05|  27.94|   4.89|0.84|      3.41| 0.44|
			|  8|   Mumbai|      India|     3.67|  18.36|   3.67|2.45|      2.49| 0.46|
			|  9|  Beijing|      China|     4.19|  30.73|   5.59| 1.4|      4.55| 0.51|
			+---+---------+-----------+---------+-------+-------+----+----------+-----+ 
		 */

		
		spark.sql("SELECT AVG(cheapmeal) AS Average_Cost,country FROM LivingCost GROUP BY country").show();
		
		/*
		 * +------------------+-----------+
			|      Average_Cost|    country|
			+------------------+-----------+
			|              3.17|       Chad|
			|10.435867768595045|     Russia|
			|3.9519999999999995|   Paraguay|
			|               8.0|   Anguilla|
			|              6.15|      Yemen|
			|3.9533333333333336|    Senegal|
			|10.341666666666665|     Sweden|
			|              5.49|     Guyana|
			| 2.933918918918921|Philippines|
			|              11.4|    Eritrea|
			|             27.03|     Jersey|
			|              9.17|   Djibouti|
			|              40.0|      Tonga|
			|2.3776923076923073|   Malaysia|
			|             10.92|  Singapore|
			|              3.75|       Fiji|
			|2.7773333333333343|     Turkey|
			|3.6933333333333334|     Malawi|
			| 4.627272727272727|       Iraq|
			| 11.60214285714286|    Germany|
			+------------------+-----------+
		 */
		
		spark.sql("SELECT country,max(pepsi) as MaxPepsi,min(pepsi) as MinPepsi,max(cappuccino) "
				+ "as MaxCappuccino,min(cappuccino) as MinCappuccino FROM LivingCost GROUP BY country").show();
		
		/*
		 * +-----------+--------+--------+-------------+-------------+
			|    country|MaxPepsi|MinPepsi|MaxCappuccino|MinCappuccino|
			+-----------+--------+--------+-------------+-------------+
			|       Chad|    0.87|    0.87|         1.72|         1.72|
			|     Russia|    2.29|     0.5|         3.42|          1.2|
			|   Paraguay|    0.88|    0.62|          1.9|         0.97|
			|   Anguilla|     3.0|     3.0|          5.0|          5.0|
			|      Yemen|     3.6|    1.05|          2.9|          2.0|
			|    Senegal|    1.09|    0.48|         3.28|         2.38|
			|     Sweden|    2.36|    0.95|         5.02|         1.91|
			|     Guyana|    1.16|    1.16|         3.49|         3.49|
			|Philippines|    1.79|    0.27|         4.42|         0.88|
			|    Eritrea|    1.56|    1.56|         1.06|         1.06|
			|     Jersey|    2.22|    2.22|         3.93|         3.93|
			|   Djibouti|    0.67|    0.66|         1.88|         1.88|
			|      Tonga|    2.11|     1.5|         2.25|         2.25|
			|   Malaysia|     0.7|    0.33|         3.33|         1.22|
			|  Singapore|    1.46|    1.46|         4.44|         4.44|
			|       Fiji|    1.05|    1.05|          2.7|         2.67|
			|     Turkey|    1.14|    0.34|         3.08|         0.54|
			|     Malawi|    1.17|    0.67|          2.3|         2.18|
			|       Iraq|    0.78|    0.25|          3.5|         0.89|
			|    Germany|    3.64|    1.04|         4.47|          1.7|
			+-----------+--------+--------+-------------+-------------+
		 */
		
		spark.close();

	}

}