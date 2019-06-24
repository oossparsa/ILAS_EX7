package de.fraunhofer.iais.kd.bda.spark;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Date;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class WordCount {
	public static void main(String[] args) {
	//	String inputFile= "/home/livlab/data/last-fm-sample1000000.tsv";
		String inputFile = "resources/sorted_sample.tsv";
	//	String inputFile= "resources/hello.txt";
		String appName = "WordCount";
	
		SparkConf conf  = new SparkConf().setAppName(appName)
										 .setMaster("local[*]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		//Split lines into words
		JavaRDD<String> words = input.flatMap(line->
		{String[] parts = line.split(" ");return Arrays.asList(parts).iterator();});
		
		JavaPairRDD<String, Integer> wordOne = words.mapToPair(word -> 
	{return new Tuple2<String,Integer>(word, (Integer)(1) );});

		JavaPairRDD<String, Integer> articCount = wordOne.reduceByKey((a,b) ->  a + b);

		String resultCount = articCount.count()+"";
		System.out.println(resultCount);
		
		// Parsa Badiei	\start
		try {
			FileWriter writeOut = new FileWriter("./tmp/wordcount_result.txt",true);
			writeOut.append(java.time.LocalTime.now()+"\t\t"+resultCount+"\n");
			writeOut.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Parsa Badiei \end
		
		articCount.saveAsTextFile("./tmp/wordcount.txt");
		context.close();

	}
}
