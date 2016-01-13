package it.polimi.spark.twostages;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.random.RandomRDDs;

public class InputGenerator {
	
	public static void main(String[] args) {
		if (args.length < 4) {
			System.out.println("usage: <output1> <output2> <n> <partitions>");
			System.exit(0);
		}
		String output1 = args[0];
		String output2 = args[1];        
        int n = Integer.parseInt(args[2]);
        int partitions = Integer.parseInt(args[3]);

		SparkConf conf = new SparkConf().setAppName("Two Stages Input Generator");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaDoubleRDD vector1 = RandomRDDs.normalJavaRDD(sc, n,partitions);
		vector1.saveAsTextFile(output1);
		
		JavaDoubleRDD vector2 = RandomRDDs.normalJavaRDD(sc, n,partitions);
		vector2.saveAsTextFile(output2);
	}

}
