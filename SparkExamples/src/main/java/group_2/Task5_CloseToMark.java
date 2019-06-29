//@CopyRight -> Parsa Badiei
//group #2 - Parsa Badiei - MA Masoud - Christian Zaharia
package group_2;


import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
		 Logger.getLogger("org").setLevel(Level.OFF);
		 Logger.getLogger("akka").setLevel(Level.OFF);
		//read the pair RDD file saved from Task4
		JavaPairRDD<String, Set<String>> user_artist_pair = 
				JavaPairRDD.fromJavaRDD(context.objectFile("/media/ILAS_ex7_saved/Task4_res"));
		//check to see if the read file is authentic 
		System.out.println("sizeofreadfile: "+user_artist_pair.count());
		
		//PairRDD files are converted to the Userset object
		JavaRDD<UserSet> rdduserset = user_artist_pair.flatMap(pair -> {return Arrays.asList( new UserSet(pair._1,pair._2)).iterator();});
		
		//collect all teh Userset objects into a list
		List<UserSet> listuserset = rdduserset.collect();
		
		//print the artist name from the Userset object
		UserSet MarkKnopfler = null;
		for (UserSet obj : listuserset) {
			if(obj.Artist.equalsIgnoreCase("Mark Knopfler")) {
				MarkKnopfler = obj;
			}
		}
		System.out.println("Mark Knopfler found! distance to himself: "+ MarkKnopfler.distanceTo(MarkKnopfler.userSet));
		Double similarityToMark = 0.15;
		String resTofile="Artist closer than"+ similarityToMark*100 +"% to Mark Knopfler:\n";
		
		//calculating similarity using List and Java directly:
		for(UserSet artist : listuserset) {
			if(MarkKnopfler.distanceTo(artist.userSet) >= similarityToMark) {
				resTofile+= artist.Artist+"\n";
				//System.out.println(artist.Artist);
			}
		}
		
		//calculating the similarity using RDD and Spark:
		final Set<String> MarkSet = MarkKnopfler.userSet;
		JavaPairRDD<String, Double> mapOfSimilarity = rdduserset.mapToPair(user -> {return new Tuple2<String,Double>(user.Artist,user.distanceTo(MarkSet));});
		System.out.println("map count "+ mapOfSimilarity.count());
		Map<String, Double> MOS =  mapOfSimilarity.collectAsMap();
		String artist_dist_Map = "Distance of all Artists to Mark Knopfler:\n";
		for (Map.Entry<String, Double> kv : MOS.entrySet()) {
			artist_dist_Map+=kv.getKey()+"="+kv.getValue()+"\n";
			System.out.println(kv.getKey()+"="+kv.getValue());
		}
		//writing the results to the file
		try {
			FileWriter writer = new FileWriter("/media/ILAS_ex7_saved/Task5_res("+ similarityToMark*100 +"%).txt",false);
			FileWriter writer2 = new FileWriter("/media/ILAS_ex7_saved/Task5_FullDistanceMap.txt",false);
			writer.write(resTofile);
			writer2.write(artist_dist_Map);
			writer.flush();
			writer2.flush();
			writer2.close();
			writer.close();
		} catch (IOException e) {e.printStackTrace();}

		context.close();
	}
	

}
