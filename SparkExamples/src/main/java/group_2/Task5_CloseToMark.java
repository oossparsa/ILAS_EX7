package group_2;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Task5_CloseToMark {

	public static void main(String[] args) {
		//String inputFile= "./tmp/user-artist-pair.txt";
		String AppName = "Task5-closeToMark-Group#2";
		SparkConf conf = new SparkConf().setAppName(AppName).setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);
		//read the pair RDD file saved from Task4
		JavaPairRDD<String, Set<String>> user_artist_pair = 
				JavaPairRDD.fromJavaRDD(context.objectFile("./tmp/user-artist-pair.obj"));
		System.out.println("hoooooooooooooooy");
		System.out.println(user_artist_pair.count());
		for(Tuple2<String, Set<String>> pair : user_artist_pair.collect()) {
			System.out.println(pair._1+" \t"+pair._2);}
		
		
		context.close();
	}
	
	public static UserSet create_profiles(JavaPairRDD<String, Set<String>> input) {
		ArrayList<UserSet> usersetsss = new ArrayList<UserSet>();
		//??? yes correct !
		JavaRDD<UserSet> list_of_artists = input.flatMap(pair -> {return Arrays.asList( new UserSet(pair._1,pair._2)).iterator();});
		// new UserSet(pair._1,pair._2)
		return null;
	}

}
