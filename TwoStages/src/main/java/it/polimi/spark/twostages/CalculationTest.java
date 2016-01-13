package it.polimi.spark.twostages;

import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.util.Random;

public class CalculationTest {

	static final Logger logger = LoggerFactory.getLogger(CalculationTest.class);
	private static final double MAX_FACTOR = 100;

	public static void main(String[] args) {

		StopWatch timer = new StopWatch();
		logger.info("Factor , Time");
		for (double factor = 1; factor < MAX_FACTOR; factor++) {
			timer.start();
			calculate(new Random().nextDouble(), factor);
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
