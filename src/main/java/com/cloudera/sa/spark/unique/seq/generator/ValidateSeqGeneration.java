package com.cloudera.sa.spark.unique.seq.generator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

//hadoop jar SeqGenerator.jar com.cloudera.sa.spark.unique.seq.generator.ValidateSeqGeneration local[2] hdfs:///user/root/op/1
public class ValidateSeqGeneration {
	public static void main(String[] args) {
		if (args.length == 0) {

		}
		if (args.length == 0) {
			System.out.println("ValidateSeqGeneration {master} {inputPath}");
		}

		String master = args[0];
		String inputPath = args[1];

		JavaSparkContext jsc = new JavaSparkContext(master,
				"ValidateSeqGeneration");

		MinMaxAccumulator minMaxAccumulator = new MinMaxAccumulator();
		Accumulable<MinMaxAccumulator, Long> accumulable = jsc.accumulable(
				minMaxAccumulator, new MinMaxAccumulatorParam());

		JavaRDD<String> javaRdd = jsc.textFile(inputPath);
		javaRdd.foreach(new ForEachMapperPartitionCounter(accumulable));

		minMaxAccumulator = accumulable.value();
		
		TreeMap<Long, Long> treeMap = new TreeMap<Long, Long>();
		
		for (Tuple2<Counter, Counter> minMax: minMaxAccumulator.minMaxRanges) {
			treeMap.put(minMax._1.val, minMax._2.val);
		}
		
		
		System.out.println("------");
		for (Entry<Long, Long> entry: treeMap.entrySet()) {
			System.out.println(entry.getKey() + "," + entry.getValue());
		}
		System.out.println("------");
	
	}
	
	public static class ForEachMapperPartitionCounter extends VoidFunction<String> {

    	Accumulable<MinMaxAccumulator, Long> accumulable;
    	transient boolean isFirst = true;
    	
    	public ForEachMapperPartitionCounter(Accumulable<MinMaxAccumulator, Long> accumulable) {
    		this.accumulable = accumulable;
    	}
    	
		@Override
		public void call(String line) throws Exception {
			long id = Long.parseLong(line.substring(1, line.indexOf(']')));
			accumulable.add(id);
		}
    	
    }

	public static class MinMaxAccumulator implements Serializable {
		ArrayList<Tuple2<Counter, Counter>> minMaxRanges = new ArrayList<Tuple2<Counter, Counter>>();

		Tuple2<Counter, Counter> localMinMax = new Tuple2<Counter, Counter>(
				new Counter(Long.MAX_VALUE), new Counter(Long.MIN_VALUE));

		public void add(Long id) {
			if (minMaxRanges.size() == 0) {
				minMaxRanges.add(localMinMax);
			}
			if (localMinMax._1.val > id) {
				localMinMax._1.val = id;
			}
			if (localMinMax._2.val < id) {
				localMinMax._2.val = id;
			}
		}

		public void add(MinMaxAccumulator minMaxAccumulator) {
			minMaxRanges.addAll(minMaxAccumulator.minMaxRanges);
		}
	}

	public static class MinMaxAccumulatorParam implements
			AccumulableParam<MinMaxAccumulator, Long> {

		public MinMaxAccumulator addAccumulator(MinMaxAccumulator arg0, Long arg1) {
			arg0.add(arg1);
			return arg0;
		}

		public MinMaxAccumulator addInPlace(MinMaxAccumulator arg0, MinMaxAccumulator arg1) {
			arg0.add(arg1);
			return arg0;
		}

		public MinMaxAccumulator zero(MinMaxAccumulator arg0) {
			return arg0;
		}

	}

	public static class Counter implements Serializable {
		public long val;

		public Counter(long val) {
			this.val = val;
		}

		public String toString() {
			return Long.toString(val);
		}
	}
}
