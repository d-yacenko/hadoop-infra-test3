package hadoop.infra.test3;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WebApp {
	// https://sparkbyexamples.com/spark/spark-read-write-files-from-hdfs-txt-csv-avro-parquet-json/
	// https://www.baeldung.com/apache-spark
	// https://dzone.com/articles/the-magic-of-apache-spark-in-java-1
	
	public static void main(String[] args) {
		 if (args.length < 1) {
		        System.err.println("Usage: JavaWordCount <file>");
		        System.exit(1);
		    }

		    SparkConf sparkConf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory","1g");;
		    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		    
		    JavaRDD<String> lines = ctx.textFile("hdfs://master-0.arena.ru:8020/tmp/hamlet.txt");

		    JavaRDD<String> words 	
		      = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		    JavaPairRDD<String, Integer> ones 
		      = words.mapToPair(word -> new Tuple2<>(word, 1));
		    JavaPairRDD<String, Integer> counts 
		      = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

		    List<Tuple2<String, Integer>> output = counts.collect();
		    for (Tuple2<?, ?> tuple : output) {
		        System.out.println(tuple._1() + ": " + tuple._2());
		    }
		    ctx.stop();
	}
}
