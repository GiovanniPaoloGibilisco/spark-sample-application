package it.polimi.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class TwoSourcesJoin {
	static final Logger logger = LoggerFactory
			.getLogger(TwoSourcesJoin.class);
	private static FileSystem hadoopFileSystem;

	public static void main(String[] args) throws IOException, InterruptedException {
		JavaSparkContext sparkContext = null;

		Config.init(args);
		Config config = Config.getInstance();
		Configuration hadoopConf = new Configuration();
		hadoopFileSystem = FileSystem.get(hadoopConf);
		SparkConf sparkConf = new SparkConf()
				.setAppName("two-sources-join");
		if (config.runLocal)
			sparkConf.setMaster("local[1]");
		sparkContext = new JavaSparkContext(sparkConf);
		
		
		if (!hadoopFileSystem.exists(new Path(config.firstFile)))
			throw new IOException(config.firstFile	+ " does not exist");
		if (!hadoopFileSystem.exists(new Path(config.secondFile)))
			throw new IOException(config.secondFile + " does not exist");
		
		
		JavaRDD<String> firstRDD = sparkContext.textFile(config.firstFile);
		JavaPairRDD<String, String> firstPairRDD = firstRDD.mapToPair(new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String element) throws Exception {
				String[] splits = element.split(",");
				return new Tuple2<String, String>(splits[0], splits[1]);
			}
		});
		
		JavaRDD<String> secondRDD = sparkContext.textFile(config.secondFile);
		JavaPairRDD<String, String> secondPairRDD = secondRDD.mapToPair(new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String element) throws Exception {
				String[] splits = element.split(",");
				return new Tuple2<String, String>(splits[0], splits[1]);
			}
		});
		
		JavaPairRDD<String, Tuple2<String, String>> joinedRDD = firstPairRDD.join(secondPairRDD);
		
		joinedRDD.saveAsTextFile(config.outputPathBase);
		
		Thread.sleep(config.waitTime);
		
		sparkContext.close();

	}

}
