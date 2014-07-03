package com.cloudera.sa.spark.unique.seq.generator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

//hadoop jar SeqGenerator.jar com.cloudera.sa.spark.unique.seq.generator.UniqueSeqGenerator local[2] hdfs:///user/root/seqdata hdfs:///user/root/op/1
public class UniqueSeqGenerator 
{
    public static void main( String[] args )
    {
        if (args.length == 0) {
        	
        }
        if (args.length == 0) {
    		System.out.println("UniqueSeqGenerator {master} {inputPath} {outputFolder}");
    	}
    	
    	String master = args[0];
    	String inputPath = args[1];
    	String outputPath = args[2];
    	
    	JavaSparkContext jsc = new JavaSparkContext(master, "UniqueSeqGenerator", null, "SeqGenerator.jar");
    	
    	SeqMapCounter seqMapCounter = new SeqMapCounter();
    	Accumulable<SeqMapCounter, String> accumulable = jsc.accumulable(seqMapCounter, new SeqMapCounterAccumulableParam());
    	
    	JavaRDD<String> javaRdd = jsc.textFile(inputPath);
    	javaRdd.foreach(new ForEachMapperPartitionCounter(accumulable));
    	
    	seqMapCounter = accumulable.value();
    	
    	System.out.println("--------");
    	System.out.println(seqMapCounter.getSummery());
    	System.out.println("--------");
    	
    	Broadcast<SeqMapCounter> broadcast = jsc.broadcast(seqMapCounter);
    	
    	JavaRDD<String> seqRdd = javaRdd.map(new MapAssignSequence(broadcast));
    	seqRdd.saveAsTextFile(outputPath);
    }
    
    public static class MapAssignSequence extends Function<String, String> {

    	Broadcast<SeqMapCounter> broadcast;
    	
    	boolean isFirst = true;
    	transient long startingIndex = 0;
    	
    	public MapAssignSequence(Broadcast<SeqMapCounter> broadcast) {
    		this.broadcast = broadcast;
    		isFirst = true;
    	}
    	
		@Override
		public String call(String line) throws Exception {
			if (isFirst) {
				isFirst = false;
				startingIndex = broadcast.value().getStartingIndex(line);
			}
			return "[" + startingIndex++ + "]" + line;
		}
    	
    }
    
    public static class ForEachMapperPartitionCounter extends VoidFunction<String> {

    	Accumulable<SeqMapCounter, String> accumulable;
    	transient boolean isFirst = true;
    	
    	public ForEachMapperPartitionCounter(Accumulable<SeqMapCounter, String> accumulable) {
    		this.accumulable = accumulable;
    	}
    	
		@Override
		public void call(String line) throws Exception {
			accumulable.add(line);
		}
    	
    }
    
    public static class SeqMapCounterAccumulableParam implements AccumulableParam<SeqMapCounter, String> {

		public SeqMapCounter addAccumulator(SeqMapCounter arg0, String arg1) {
			arg0.add(arg1);
			return arg0;
		}

		public SeqMapCounter addInPlace(SeqMapCounter arg0, SeqMapCounter arg1) {
			arg0.add(arg1);
			return arg0;
		}

		public SeqMapCounter zero(SeqMapCounter arg0) {
			return arg0;
		}

		
    }
    
    public static class SeqMapCounter implements Serializable{
    	HashMap<String, Counter> counterMap = new HashMap<String, Counter>();
    	
    	Counter currentCounter = null;
    	
    	public void add(String lineKey) {
    		if (currentCounter == null) {
    			currentCounter = new Counter();
    			counterMap.put(lineKey, currentCounter);
    		}
    		currentCounter.val++;
    	}
    	
    	public void add(SeqMapCounter seqMapCounter) {
    		counterMap.putAll(seqMapCounter.counterMap);
    	}
    	
    	public long getStartingIndex(String line) {
    		long startingIndex = 0;
    		
    		TreeMap<String, Counter> treeMap = new TreeMap<String, Counter>();
    		treeMap.putAll(counterMap);
    		
    		for (Entry<String, Counter> entry: treeMap.entrySet()) {
    			if (entry.getKey().compareTo(line) < 0) {
    				startingIndex += entry.getValue().val;
    			}
    		}
    		return startingIndex;
    	}
    	
    	public String getSummery() {
    		StringBuilder strBuilder = new StringBuilder();
    		for (Entry<String, Counter> entry: counterMap.entrySet()) {
    			strBuilder.append("(" + entry.getKey() + "," + entry.getValue() + ")");
    		}
        	return strBuilder.toString();
    	}
    }
    
    

    public static class Counter implements Serializable{
    	public long val = 0;
    	
    	public String toString() {
    		return Long.toString(val);
    	}
    }
    

}
