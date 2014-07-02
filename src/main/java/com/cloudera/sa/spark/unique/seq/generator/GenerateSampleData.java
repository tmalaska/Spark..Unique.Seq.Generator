package com.cloudera.sa.spark.unique.seq.generator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


//hadoop jar SeqGenerator.jar com.cloudera.sa.spark.unique.seq.generator.GenerateSampleData 1000000 10 seqdata  
public class GenerateSampleData {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("GenerateSampleData <numberOfRecords> <numberOfFiles> <outputFolder>");
			return;
		}
		
		int numberOfRecords = Integer.parseInt(args[0]);
		int numberOfFiles = Integer.parseInt(args[1]);
		String outputRootFolder = args[2];
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		
		Random r = new Random();
		
		for (int i = 0; i < numberOfFiles; i++) {
			Path outputPath = new Path(outputRootFolder + "/file." + i + ".txt");
			
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(
							fs.create(
									outputPath, 
									true, 
									config.getInt("io.file.buffer.size", 4096), 
									(short)3, 
									(long)(32 * 1024 * 1024))));
			
			System.out.println("Writing to " + outputPath);
			
			for (int j = 0 ; j < numberOfRecords; j++) {
				writer.append(i + "|" + j + "|" + r.nextLong() + "|" + r.nextLong() + "|" + r.nextLong());
				writer.newLine();
				if (j % 10000 == 0) {
					System.out.print(".");
				}
			}
			System.out.println();
			writer.close();
			System.out.println("Finished " + outputPath);
		}
	}
}
