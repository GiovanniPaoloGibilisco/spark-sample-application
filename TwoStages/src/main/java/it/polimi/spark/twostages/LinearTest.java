package it.polimi.spark.twostages;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinearTest {

	static final Logger logger = LoggerFactory.getLogger(LinearTest.class);

	public static void main(String[] args) {

		int size = Integer.parseInt(args[0]);
		double factor = Double.parseDouble(args[1]);

		List<Double> input1 = new ArrayList<Double>();
		List<Double> input2 = new ArrayList<Double>();

		StopWatch timer = new StopWatch();

		initInput(input1, input2, size);
		List<Double> unifiedInput = new ArrayList<Double>(input1);
		unifiedInput.addAll(input2);
		
		timer.start();
		doWork(unifiedInput,factor);
		timer.split();
		long time = timer.getSplitTime();
		logger.info(factor+","+size*2+","+time);

	}

	private static void doWork(List<Double> unifiedInput, double factor) {
		for(double item:unifiedInput)
			calculate(item,factor);
	}

	private static void calculate(double item, double factor) {
		for(int i=0;i<factor;i++)
			Math.tan(item);
	}

	private static void initInput(List<Double> input1, List<Double> input2, int size) {
		// TODO Auto-generated method stub
		input1.clear();
		input2.clear();
		Random random = new Random();

		for (int i = 0; i < size; i++) {
			input1.add(random.nextDouble());
			input2.add(random.nextDouble());
		}
	}

}
