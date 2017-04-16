

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
public final class LogAnalysisSpark {
	
	private static final Pattern SPACE = Pattern.compile(" ");
	static String appName="LogAnalysisSpark";
	static String master="local[*]";
	
	static ArrayList<String> folders = new ArrayList<String>();
	static ArrayList<String> files = new ArrayList<String>();
	private static JavaSparkContext sc;
	private static List<Tuple2<String, Integer>> output;
	
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

		//String inputPath = "/Users/Harpreet/logAnalyze/LogAnalysis/input";
		//String outputPath = "/Users/Harpreet/logAnalyze/LogAnalysis/output";
		//String file = "/Users/Harpreet/logAnalyze/LogAnalysis/input/iliad/part-0000";
		
		int ques = 0;
		//File[] dirs = new File(inputPath).listFiles(File::isDirectory);
		//getFileAndFolders(inputPath);
		
        /*for(String f: folders){
        	getFileAndFolders(f);
        	System.out.println(f);
        }*/
		
		//getFileAndFolders(folders.get(0));

	    if (args.length < 1) {
	      System.err.println("Usage: LogAnalysisSpark <file>");
	      System.exit(1);
	    }else{
	    	ques = Integer.parseInt(args[0]);
	    	SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
	        sc = new JavaSparkContext(conf);
	        
	        SparkSession spark = SparkSession.builder().appName("LogAnalysisSpark").getOrCreate();
	    	
	    	if(ques==2){
	        	
	        	JavaRDD<String> mainRDD1 = sc.textFile("../LogAnalysis/input/"+args[1]);
	        	//System.out.println("\t\t\tmain RDD: "+a1+"\t\t"+folders.get(0));
	            JavaRDD<String> lines0 = mainRDD1
	            		.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
	            		.filter(s -> s.equalsIgnoreCase(args[1]));
	
	            JavaPairRDD<String, Integer> ones0 = lines0.mapToPair(s -> new Tuple2<>(s, 1));
	            
	            JavaPairRDD<String, Integer> counts0 = ones0.reduceByKey((i1, i2) -> i1 + i2);
	            //counts0.saveAsTextFile(outputPath+"/"+1);
	        	JavaRDD<String> mainRDD2 = sc.textFile("../LogAnalysis/input/"+args[2]);
	        	//System.out.println("\t\t\tmain RDD: "+a2+"\t\t"+folders.get(1));
	           
	        	JavaRDD<String> lines1 = mainRDD2
	            		.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
	            		.filter(s -> s.equalsIgnoreCase(args[2]));
	
	            JavaPairRDD<String, Integer> ones1 = lines1.mapToPair(s -> new Tuple2<>(s, 1));
	            JavaPairRDD<String, Integer> counts1 = ones1.reduceByKey((i1, i2) -> i1 + i2);
	            JavaPairRDD<String, Integer> counts2 = counts0.union(counts1);
	            
	            output = (counts0.union(counts1)).collect();
	            //counts2.saveAsTextFile(outputPath+"/"+1);
	            // For printing result on console
	                System.out.println("* Q1: line counts");
	             for (Tuple2<?,?> tuple : output) {
	                System.out.println("   + "+tuple._1() + ": " + tuple._2());
	             }
	    		
	    	}else if(ques==1){
	    		String user="achille";//achille
	    		JavaRDD<String> mainRDD1 = sc.textFile("../LogAnalysis/input/"+args[1]);
	        	System.out.println("\t\t RDD:\t\t"+args[1]);
	            JavaRDD<String> lines0 = mainRDD1
	            		.filter(s -> s.contains("Starting"))
	            		.filter(s -> s.contains("Session"))
	            		.filter(s -> s.contains(user));
	        	
	        	/*avaRDD<String> lines0 = mainRDD1
	            		.filter(s -> s.contains("Starting"));
	        	System.out.println("lines0: "+lines0.count());
	        	JavaRDD<String> lines1 = lines0
	            		.filter(s -> s.contains("Session"));
	        	System.out.println("lines1: "+lines1.count());
	        	JavaRDD<String> lines2 = lines1
	            		//.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
	            		.filter(s -> s.contains(user));*/
	
	            long ones0 = lines0.count();
	            
	        	JavaRDD<String> mainRDD2 = sc.textFile("../LogAnalysis/input/"+args[2]);
	        	System.out.println("\t\t RDD:\t\t"+args[2]);
	           
	        	JavaRDD<String> lines1 = mainRDD2
	            		.filter(s -> s.contains("Starting"))
	            		.filter(s -> s.contains("Session"))
	            		.filter(s -> s.contains(user));
	
	            long ones1 = lines1.count();
	            //counts2.saveAsTextFile(outputPath+"/"+1);
	            // For printing result on console
                System.out.println("* Q2: sessions of user ’achille’");
                System.out.println("   + "+args[1] + ": " + ones0);
                System.out.println("   + "+args[2] + ": " + ones1);
                
	             
	    	}else if(ques==3){
	    		
	    	}else if(ques==4){
	    		
	    	}else if(ques==5){
	    		
	    	}else if(ques==6){
	    		
	    	}else if(ques==7){
	    		
	    	}else if(ques==8){
	    		
	    	}else if(ques==9){
	    		
	    	}else{
	    		
	    	}
	    	
	
	      sc.stop();
	      spark.stop();
	    }
  }
}
