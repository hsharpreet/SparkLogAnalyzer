

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Pattern;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
public final class LogAnalysisSpark {
	
	private static final Pattern SPACE = Pattern.compile(" ");
	static String appName="LogAnalysisSpark";
	static String master="local[1]";
	static ArrayList<String> folders = new ArrayList<String>();
	static ArrayList<String> files = new ArrayList<String>();
	
	public static void getFileAndFolders(String s){
		  File folder = new File(s);
		  File[] listOfFiles = folder.listFiles();
		
		  for (int i = 0; i < listOfFiles.length; i++) {
			  if (listOfFiles[i].isFile()) {
				  if(!listOfFiles[i].getAbsolutePath().contains("DS_Store")){
					  files.add(listOfFiles[i].getAbsolutePath());
				  }
			  }else if (listOfFiles[i].isDirectory()) {
				  folders.add(listOfFiles[i].getAbsolutePath());
			  }
		  }
	} 
  
	public static void main(String[] args) throws Exception {

		String path = "/Users/Harpreet/Documents/STS_WS/LogAnalysis/input";
		File[] dirs = new File(path).listFiles(File::isDirectory);
		getFileAndFolders(path);
		
		System.out.println("folders: \n");
        for(String f: folders){
        	getFileAndFolders(f);
        	System.out.println(f);
        }
        System.out.println("files: \n");
        for(String f: files){
        	System.out.println(f);
        }

    if (args.length < 1) {
      System.err.println("Usage: LogAnalysisSpark <file>");
      System.exit(1);
    }
    
    SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    SparkSession spark = SparkSession.builder().appName("LogAnalysisSpark").getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>("iliad", 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    counts.saveAsTextFile("file:///Users/Harpreet/Documents/STS_WS/LogAnalysis/output/1)");
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    spark.stop();
  }
}
