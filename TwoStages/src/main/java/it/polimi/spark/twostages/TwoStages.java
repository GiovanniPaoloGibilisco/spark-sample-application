package it.polimi.spark.twostages;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoStages {
	static final Logger logger = LoggerFactory.getLogger(TwoStages.class);

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("usage: <input1> <input2> <output> <linearFactor>");
			System.exit(0);
		}
		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];
		String factorS = args[3];

		double factor = Double.parseDouble(factorS);

		SparkConf conf = new SparkConf().setAppName("Two Stages");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// load the first RDD
		JavaRDD<String> data1 = sc.textFile(input1);
		JavaRDD<Double> doubleVect1 = data1.map(new Function<String, Double>() {
			@Override
			public Double call(String arg0) throws Exception {
				return Double.parseDouble(arg0);
			}
		});

		// load the second RDD
		JavaRDD<String> data2 = sc.textFile(input2);
		JavaRDD<Double> doubleVect2 = data2.map(new Function<String, Double>() {
			@Override
			public Double call(String arg0) throws Exception {
				return Double.parseDouble(arg0);
			}
		});

		// do the linear computation on the union of the two rdds
		JavaRDD<Double> unitedData = doubleVect1.union(doubleVect1);
		unitedData.map(new Function<Double, Double>() {
			@Override
			public Double call(Double arg0) throws Exception {
				for (int i = 0; i < factor; i++)
					Math.tan(arg0);
				return Math.tan(arg0);
			}
		});
		logger.info("Liner Done");

		// do the cartesian product
		JavaPairRDD<Double, Double> cartesian = doubleVect1.cartesian(doubleVect2);
		logger.info("Cartesian Done");
		
		//perform some dummy operation including both RDDs
		long count = cartesian.keys().union(unitedData).count();
		logger.info("Count: "+count);

	}

}
