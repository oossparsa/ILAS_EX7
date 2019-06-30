package group_2;


import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Ex8_Task3 {

	public static void main(String[] args) {
		String AppName = "Task3-minHashSignature-Group#2";
		SparkConf conf = new SparkConf().setAppName(AppName).setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		//read the pair RDD file saved from Task6 (the RDD of the userset objects)
		JavaRDD<UserSet> RDDuserset = context.objectFile("/media/ILAS_ex7_saved/RDD_Userset");
		//System.out.println(RDDuserset.count());
		JavaPairRDD<String, String> minHashSgnatures = RDDuserset.mapToPair(obj -> {return new Tuple2<String,String>(obj.Artist,obj.toMinHashSignature());});
		//minHashSgnatures.saveAsTextFile("/media/ILAS_ex7_saved/minHashSigs");
		//to print in a more readable way
		Map<String,String> signatures = minHashSgnatures.collectAsMap();
		for(Map.Entry<String, String> pair : signatures.entrySet()) {
			System.out.println(pair.getKey()+"\t["+pair.getValue()+"]");
		}

		context.close();
	}

}
