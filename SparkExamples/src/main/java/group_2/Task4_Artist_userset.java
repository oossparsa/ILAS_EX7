//@CopyRight -> Parsa Badiei
//group #2 - Parsa Badiei - MA Masoud - Christian Zaharia
package group_2;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Task4_Artist_userset {
public static void main(String[] arg) {
	//String inputFile = "resources/sorted_sample.tsv";
	String inputFile= "resources/last-fm-sample100000.tsv";
	String AppName = "Artist-UserSet-Group#2";
	
	SparkConf conf = new SparkConf().setAppName(AppName).setMaster("local[*]");
	JavaSparkContext context = new JavaSparkContext(conf);
	//read the input text file
	JavaRDD<String> input = context.textFile(inputFile);
	//split the lines in the text file and take the artist name and user name and create a pair (key,value)
	//Value in the pair is a hash set containing the user for the read event 
	JavaPairRDD<String,Set<String>> user_artist = input.mapToPair(line->
	{return new Tuple2<String, Set<String>>(line.split("\t")[3],new HashSet<String>(Arrays.asList(line.split("\t")[0])));});

	//aggregate by key -> the value of the pair would be a set of all the users for the Artist(Key)
	JavaPairRDD<String, Set<String>> reduced_user_artist = user_artist.aggregateByKey(new HashSet<String>(), (val,agg)->mergesets(agg, val), (agg1,agg2)->mergesets(agg1, agg2));
	//print results
	System.out.println("Artist\tUser\t\t"+LocalDateTime.now());
	String textToFile = "";

	for(Tuple2<String, Set<String>> pair : reduced_user_artist.collect()) {
		System.out.println(pair._1+" \t"+pair._2);
		textToFile += pair._1+" \t\t "+pair._2+"\n";
	}
	//write results to a file
	//reduced_user_artist.saveAsObjectFile("./tmp/user-artist-pair.obj");
	//input.saveAsObjectFile("./tmp/user-artist-pair.obj");
	reduced_user_artist.saveAsObjectFile("/media/ILAS_ex7_saved/Task4_res");
	try {FileWriter fwrite = new FileWriter("./tmp/Task4_result.txt",false);fwrite.write("Artist\tUser\t\t"+LocalDateTime.now()+"\n");fwrite.write(textToFile);fwrite.flush();fwrite.close();} catch (IOException e) {e.printStackTrace();}
	context.close();
}
public static HashSet<String> mergesets(Set<String> aset, Set<String> bset)
{aset.addAll(bset); return new HashSet<String>(aset);}
}
