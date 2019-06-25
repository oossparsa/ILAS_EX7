//@CopyRight -> Parsa Badiei
//group #2 - Parsa Badiei - MA Masoud - Christian Zaharia
package group_2;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Task5_CloseToMark {

	public static void main(String[] args) {
		String AppName = "Task5-closeToMark-Group#2";
		SparkConf conf = new SparkConf().setAppName(AppName).setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);
		//read the pair RDD file saved from Task4
		JavaPairRDD<String, Set<String>> user_artist_pair = 
				JavaPairRDD.fromJavaRDD(context.objectFile("/media/ILAS_ex7_saved/Task4_res"));
		//check to see if the read file is authentic 
		System.out.println("sizeofreadfiel: "+user_artist_pair.count());
		
		//PairRDD files are converted to the Userset object
		JavaRDD<UserSet> rdduserset = user_artist_pair.flatMap(pair -> {return Arrays.asList( new UserSet(pair._1,pair._2)).iterator();});
		
		//collect all teh Userset objects into a list
		List<UserSet> listuserset = rdduserset.collect();
		System.out.println("sizeofrddinlist:  "+listuserset.size());
		
		//print the artist name from the Userset object
		for (UserSet obj : listuserset) {
			System.out.println(obj.Artist);
		}
		System.out.println("sizeofrddinlist:  "+listuserset.size());
		
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
