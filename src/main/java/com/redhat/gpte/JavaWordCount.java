/**
 * 
 */
package com.redhat.gpte;

/**
 * @author prakrish
 *
 */
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
public class JavaWordCount {
	public static void main(String[] args) throws Exception {
		//String inputFile = "src/main/resources/input.txt";
		//Initialize Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local[*]"));
		// Load data from Input File. Generic Input Directory not from IDE Perspective, In case of Hadoop Platform, Please use this below.
		JavaRDD<String> input = sc.textFile("/tmp/input.txt");
		// Split up into words.
		JavaPairRDD<String, Integer> counts = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		System.out.println(counts.collect());
		sc.stop();
		sc.close();
	}
}
