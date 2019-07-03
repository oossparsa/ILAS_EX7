package group_2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Ex8_Task4 {

	public static void main(String[] args) {
		String AppName = "Task3-minHashSignature-Group#2";
		SparkConf conf = new SparkConf().setAppName(AppName).setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		//read the pair RDD file saved from Task6 (the RDD of the userset objects)
		JavaRDD<UserSet> RDDuserset = context.objectFile("/media/ILAS_ex7_saved/RDD_Userset");
		
		//calculating parts of the signature in each map
		JavaPairRDD<String, String> minHashSgnatures1 = RDDuserset.mapToPair(obj -> {return new Tuple2<String,String>(obj.Artist,obj.toMinHashSignature(0,4));});
		JavaPairRDD<String, String> minHashSgnatures2 = RDDuserset.mapToPair(obj -> {return new Tuple2<String,String>(obj.Artist,obj.toMinHashSignature(5,9));});
		JavaPairRDD<String, String> minHashSgnatures3 = RDDuserset.mapToPair(obj -> {return new Tuple2<String,String>(obj.Artist,obj.toMinHashSignature(10,14));});
		JavaPairRDD<String, String> minHashSgnatures4 = RDDuserset.mapToPair(obj -> {return new Tuple2<String,String>(obj.Artist,obj.toMinHashSignature(15,19));});
		
	}
	

}
