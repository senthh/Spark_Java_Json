//import java.util.Arrays;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

public class Spark_Json { 
	  public static void main(String[] args) { 
		  
		  SparkConf conf =new SparkConf().setMaster("local").setAppName("Json_Reader"); 
		  conf.set("spark.sql.crossJoin.enabled", "true");
		  JavaSparkContext javaSparkContext = new JavaSparkContext(conf); 
		  
		  SparkSession sparkSession = SparkSession
			      .builder()
			      .master("local")
				  .appName("JavaJsonExample")
				  .getOrCreate();
		  
		  
		  JavaPairRDD<String,String> assetsContentsRDD  = javaSparkContext.wholeTextFiles("/Users/senthilkumar/Documents/Senthil/RiskSense/asset/",20);
		 
		  JavaRDD<String> assetsContents = assetsContentsRDD.map(new Function<Tuple2<String, String>, String>() {
	            @Override
	            public String call(Tuple2<String, String> assetContent) throws Exception {
	                return assetContent._2();
	               
	            }
		  });
		  
		  Dataset<Row> json_rec=sparkSession.read().option("multiline", "true").json(assetsContents);
		  
		  
		  JavaPairRDD<String,String> qualys_kb  = javaSparkContext.wholeTextFiles("/Users/senthilkumar/Documents/Senthil/RiskSense/qualys.kb.json",1);
		  JavaRDD<String> qualys_kbContents = qualys_kb.map(new Function<Tuple2<String, String>, String>() {
	            @Override
	            public String call(Tuple2<String, String> qualys_kbContent) throws Exception {
	                return qualys_kbContent._2();
	               
	            }
		  });
		  
		  Dataset<Row> json_rec_qualys_kb=sparkSession.read().option("multiline", "true").json(qualys_kbContents);
		  
		  json_rec_qualys_kb.printSchema();
		  json_rec_qualys_kb.select("VULN.CATEGORY", "VULN.QID","VULN.SEVERITY_LEVEL","VULN.VULN_TYPE","_id","dateloaded").show(100,false);
		  
		  Dataset<Row> parq_rec = json_rec.join(json_rec_qualys_kb, (json_rec.col("QID").equalTo(json_rec_qualys_kb.col("VULN.QID"))), "inner").select("dns","fqdn","ip","port","qid","VULN.SEVERITY_LEVEL");
		  
		  parq_rec.coalesce(10).write().option("parquet.block.size", "33554432")
		  				  .option("parquet.page.size", "33554432").parquet("/Users/senthilkumar/Documents/Senthil/RiskSense/asset/RiskSense_12.parquet");
		  
		  //parq_rec.coalesce(2).write().format("parquet").save("/Users/senthilkumar/Documents/Senthil/RiskSense/asset/RiskSense_data_colesce.parquet");
		  
		  //Dataset<Row> df = sparkSession.read().parquet("/Users/senthilkumar/Documents/Senthil/RiskSense/asset/RiskSense_data.parquet").toDF();
		  //df.show();
		  //parq_rec.toDF().groupBy("ip").count().show();
		  
		  //parq_rec.toDF().groupBy("port","ip").count().show();
		  
		  //parq_rec.toDF().groupBy("qid","ip").count().show();
		  
		 // parq_rec.toDF().groupBy("severity_level","ip").count().show(1000);
	  }
	  
}