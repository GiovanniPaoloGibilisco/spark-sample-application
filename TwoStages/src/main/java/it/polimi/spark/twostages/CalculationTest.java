package it.polimi.spark.twostages;

import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.util.Random;

public class CalculationTest {

	static final Logger logger = LoggerFactory.getLogger(CalculationTest.class);
	private static final double MIN_FACTOR = 100000000d;
	private static final double MAX_FACTOR = 1000000000d;

	public static void main(String[] args) {

		
		double num = new Random().nextDouble();
		logger.info("num: "+num);
		logger.info("Factor , Time");
		
		for (double factor = MIN_FACTOR; factor <= MAX_FACTOR; factor+=(MAX_FACTOR)/10) {
			StopWatch timer = new StopWatch();
			timer.start();
			calculate(num, factor);
			timer.split();			
			long time = timer.getSplitTime();
			timer.stop();

			logger.info(factor + ","+ time);
		}

	}

	private static void calculate(double item, double factor) {
		for (int i = 0; i < factor; i++)
			Math.tan(item);
	}

}
