

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

		String inputPath = "/Users/Harpreet/logAnalyze/LogAnalysis/input";
		String outputPath = "/Users/Harpreet/logAnalyze/LogAnalysis/output";
		String file = "/Users/Harpreet/logAnalyze/LogAnalysis/input/iliad/part-00000";
		
		File[] dirs = new File(inputPath).listFiles(File::isDirectory);
		getFileAndFolders(inputPath);
		
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

    JavaRDD<String> textFile = sc.textFile(file);
    
    /*JavaPairRDD<String, Integer> counts = textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b);
    counts.saveAsTextFile(outputPath+"/1");*/
    
    JavaRDD<String> lines0 = textFile
    		.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
    		.filter(s -> s.equalsIgnoreCase("iliad"));

    JavaPairRDD<String, Integer> ones0 = lines0.mapToPair(s -> new Tuple2<>(s, 1));
    
    JavaPairRDD<String, Integer> counts1 = ones0.reduceByKey((i1, i2) -> i1 + i2);
    counts1.saveAsTextFile(outputPath+"/1");
    
    List<Tuple2<String, Integer>> output = counts1.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
   
    spark.stop();
  }
}
