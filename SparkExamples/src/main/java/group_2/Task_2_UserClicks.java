//@CopyRight -> Parsa Badiei
//group #2 - Parsa Badiei - MA Masoud - Christian Zaharia
package group_2;



import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class Task_2_UserClicks {
	public static void main(String[] args) {
		String inputFile= "resources/last-fm-sample100000.tsv";
	//	String inputFile = "resources/sorted_sample.tsv";
		String appName = "UserClicks-Group#2";
	
		SparkConf conf  = new SparkConf().setAppName(appName)
										 .setMaster("local[*]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		//Split lines into words
		JavaRDD<String> fields = input.flatMap(line->
		{String[] parts = line.split("\t");return Arrays.asList(parts[3]).iterator();});
		
		JavaPairRDD<String, Integer> wordOne = fields.mapToPair(word -> 
	{return new Tuple2<String,Integer>(word, (Integer)(1) );});

		JavaPairRDD<String, Integer> articCount = wordOne.reduceByKey((a,b) ->  a + b);

		String resultCount = articCount.count()+"";
		System.out.println(resultCount);
		
		//Task 2
		System.out.println("Artist\tClicks");
		String resultToFile = "Artist\t\tClicks\n";
		for (Tuple2<String, Integer> pair : articCount.collect()) {
			System.out.println(pair._1+"\t"+pair._2);
			resultToFile += '\"'+pair._1+"\" "+"\t\t"+pair._2+"\n";
		}
		
		// Parsa Badiei	\start
		try {
			FileWriter writeOut = new FileWriter("./tmp/wordcount_result.txt",true);
			writeOut.append(java.time.LocalTime.now()+"\t\t"+resultCount+"\n");
			FileWriter Task2 = new FileWriter("./tmp/Task_result.txt",false);
			Task2.write(resultToFile);
			writeOut.flush();
			Task2.flush();
			writeOut.close();
			Task2.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Parsa Badiei \end
		
		articCount.saveAsTextFile("./tmp/wordcount.txt");
		context.close();

	}
}

