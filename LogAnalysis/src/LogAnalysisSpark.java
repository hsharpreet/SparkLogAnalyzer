

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;
public final class LogAnalysisSpark {
	
	private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

	private static final Pattern SPACE = Pattern.compile(" ");
	static String appName="LogAnalysisSpark";
	static String master="local[*]";
	
	static ArrayList<String> folders = new ArrayList<String>();
	static ArrayList<String> files = new ArrayList<String>();
	private static JavaSparkContext sc;
	private static List<Tuple2<String, Integer>> output1;
	private static List<Tuple2<String, Integer>> output2;
	
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
	    	
	    	if(ques==0){
	        	
	        	JavaRDD<String> mainRDD1 = sc.textFile("../LogAnalysis/input/"+args[1]);
	        	JavaRDD<String> mainRDD2 = sc.textFile("../LogAnalysis/input/"+args[2]);
	        	//System.out.println("\t\t\tmain RDD: "+a1+"\t\t"+folders.get(0));
	            /*JavaRDD<String> lines0 = mainRDD1
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
	            
	            output1 = (counts0.union(counts1)).collect();*/
	            //counts2.saveAsTextFile(outputPath+"/"+1);
	            // For printing result on console
	        	long mainRDD1count = mainRDD1.count();
	        	long mainRDD2count = mainRDD2.count();
	                System.out.println("* Q1: line counts");
	                System.out.println("   + "+args[1]+": "+mainRDD1count);
	                System.out.println("   + "+args[2]+": "+mainRDD2count);
	             /*for (Tuple2<?,?> tuple : output1) {
	               // System.out.println("   + "+tuple._1() + ": " + tuple._2());
	             }*/
	    		
	    	}else if(ques==2){
	    		String user="achille";
	    		JavaRDD<String> mainRDD1 = sc.textFile("../LogAnalysis/input/"+args[1]);
	        	System.out.println("\t\t RDD:\t\t"+args[1]);
	            JavaRDD<String> lines0 = mainRDD1
	            		.filter(s -> s.contains("Starting"))
	            		.filter(s -> s.contains("Session"))
	            		.filter(s -> s.contains(user));
	        	
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
	    		JavaRDD<String> mainRDD0 = sc.textFile("../LogAnalysis/input/"+args[1]);
	            JavaRDD<String> lines0 = mainRDD0
	            		.filter(s -> s.contains("Starting Session"))
	            		.filter(s -> s.contains("of user"));
	           
	            JavaRDD<ApacheAccessLog> accessLogs0 =
	            	       lines0.map(ApacheAccessLog::parseFromLogLine).cache();
	            
	            JavaPairRDD<String, Integer> ones0 = accessLogs0.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
	            JavaPairRDD<String, Integer> counts0 = ones0.reduceByKey((i1, i2) -> i1 + i2);
	            output1 = counts0.collect();
	                
	             JavaRDD<String> mainRDD2 = sc.textFile("../LogAnalysis/input/"+args[2]);
		            JavaRDD<String> lines1 = mainRDD2
		            		.filter(s -> s.contains("Starting Session"))
		            		.filter(s -> s.contains("of user"));
		           
		            JavaRDD<ApacheAccessLog> accessLogs1 = lines1.map(ApacheAccessLog::parseFromLogLine).cache();
		            
		            JavaPairRDD<String, Integer> ones1 = accessLogs1.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
		            JavaPairRDD<String, Integer> counts1 = ones1.reduceByKey((i1, i2) -> i1 + i2);
		            
		            output2 = counts1.collect();

		            System.out.println("* Q3: unique user names");
	                System.out.print("   + "+args[1]+" :");
	                ArrayList<String> users = new ArrayList<String>();
		             for (Tuple2<?,?> tuple : output1) {
		            	 users.add(tuple._1().toString().substring(0, tuple._1().toString().length()-1));
		                //System.out.print("'"+tuple._1().toString().substring(0, tuple._1().toString().length()-1)+"'");
		             }
		             System.out.println(users);
	             
		            System.out.print("   + "+args[2]+" :");
	                users.clear();
		             for (Tuple2<?,?> tuple : output2) {
		            	 users.add(tuple._1().toString().substring(0, tuple._1().toString().length()-1));
		                //System.out.print("'"+tuple._1().toString().substring(0, tuple._1().toString().length()-1)+"'");
		             }
		             System.out.println(users);

	    		
	    	}else if(ques==4 || ques==1 || ques==8){
	    		JavaRDD<String> mainRDD0 = sc.textFile("../LogAnalysis/input/"+args[1]);
	            JavaRDD<String> lines0 = mainRDD0
	            		.filter(s -> s.contains("Starting Session"))
	            		.filter(s -> s.contains("of user"));
	           
	            JavaRDD<ApacheAccessLog> accessLogs0 =
	            	       lines0.map(ApacheAccessLog::parseFromLogLine).cache();
	            
	            JavaPairRDD<String, Integer> ones0 = accessLogs0.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
	            
	                
	             JavaRDD<String> mainRDD2 = sc.textFile("../LogAnalysis/input/"+args[2]);
		            JavaRDD<String> lines1 = mainRDD2
		            		.filter(s -> s.contains("Starting Session"))
		            		.filter(s -> s.contains("of user"));
		           
		            JavaRDD<ApacheAccessLog> accessLogs1 = lines1.map(ApacheAccessLog::parseFromLogLine).cache();
		            
		            JavaPairRDD<String, Integer> ones1 = accessLogs1.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
		            

		            if(ques==4){
		            	JavaPairRDD<String, Integer> counts0 = ones0.reduceByKey((i1, i2) -> i1 + i2);
			            output1 = counts0.collect();
			            
			            JavaPairRDD<String, Integer> counts1 = ones1.reduceByKey((i1, i2) -> i1 + i2);
			            output2 = counts1.collect();
			            
			            System.out.println("* Q4: sessions per user");
		                System.out.print("   + "+args[1]+" :");
		                ArrayList<String> users = new ArrayList<String>();
			             for (Tuple2<?,?> tuple : output1) {
			            	 String u="'"+tuple._1().toString().substring(0, tuple._1().toString().length()-1)+"', ";
			            	 String c= tuple._2().toString();
			            	 users.add("("+u+c+")");
			             }
			             System.out.println(users);
		             
			            System.out.print("   + "+args[2]+" :");
		                users.clear();
			             for (Tuple2<?,?> tuple : output2) {
			            	 String u="'"+tuple._1().toString().substring(0, tuple._1().toString().length()-1)+"', ";
			            	 String c= tuple._2().toString();
			            	 users.add("("+u+c+")");
			             }
			             System.out.println(users);
			             
		            }else if(ques==7){
		            	JavaPairRDD<String, Integer> intersection = ones0.intersection(ones1);
			            output1 = intersection.collect();
			            
			            System.out.println("* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.");
		                ArrayList<String> users = new ArrayList<String>();
		             
			             for (Tuple2<?,?> tuple : output1) {
			            	 users.add("'"+tuple._1().toString().substring(0, tuple._1().toString().length()-1)+"'");
			             }
			             System.out.println(users);
		            }else{
		            	
		            	
		            	JavaPairRDD<String, String> q81 = accessLogs0
		            			.mapToPair(log -> new Tuple2<>(log.getUsername(),args[1]))
		            			.reduceByKey((i1, i2) -> i1);
		            	JavaPairRDD<String, String> q82 = accessLogs1
		            			.mapToPair(log -> new Tuple2<>(log.getUsername(),args[2]))
		            			.reduceByKey((i1, i2) -> i1);
		            	
		            	JavaPairRDD<String, String> q83 = q81.cogroup(q82)
		            			.flatMapValues(new Function<Tuple2<Iterable<String>, Iterable<String>>, Iterable<String>>() {
		            			        @Override
		            			        public Iterable<String> call(Tuple2<Iterable<String>, Iterable<String>> value) {
										  ArrayList<String> list = new ArrayList<String>();
										  if (Iterables.isEmpty(value._1()) || Iterables.isEmpty(value._2())) {
										    Iterables.addAll(list, value._1());
										    Iterables.addAll(list, value._2());
										  }
										  return list;
		            			        }
		            			 });
		            	
			            List<Tuple2<String, String>> output4 = q83.collect();
			            
			            
			            System.out.println("* Q8: users who started a session on exactly one host, with host name.");
		                ArrayList<String> users = new ArrayList<String>();
		                for (Tuple2<?,?> tuple : output4) {
		                	String u="'"+tuple._1().toString().substring(0, tuple._1().toString().length()-1)+"', ";
			            	String c= "'"+tuple._2().toString()+"'";
		                	users.add("("+u+c+")");
			             }
			             System.out.println("   + : "+users);
		            }

	    		
	    	}else if(ques==5){
	    		JavaRDD<String> mainRDD1 = sc.textFile("../LogAnalysis/input/"+args[1]);
	        	System.out.println("\t\t RDD:\t\t"+args[1]);
	            JavaRDD<String> lines0 = mainRDD1
	            		//.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
	            		.filter(s -> s.toLowerCase().contains("error"));    	
	            long ones0 = lines0.count();
	            
	        	JavaRDD<String> mainRDD2 = sc.textFile("../LogAnalysis/input/"+args[2]);
	        	System.out.println("\t\t RDD:\t\t"+args[2]);
	           
	        	JavaRDD<String> lines1 = mainRDD2
	        			//.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
	            		.filter(s -> s.toLowerCase().contains("error"));
	
	            long ones1 = lines1.count();
	            //counts2.saveAsTextFile(outputPath+"/"+1);
	            // For printing result on console
                System.out.println("* Q5: number of errors");
                System.out.println("   + "+args[1] + ": " + ones0);
                System.out.println("   + "+args[2] + ": " + ones1);
	             
	    	}else if(ques==6){
	    		
	    		JavaRDD<String> mainRDD0 = sc.textFile("../LogAnalysis/input/"+args[1]);
	    		JavaRDD<String> lines0 = mainRDD0
	            		.filter(s -> s.toLowerCase().contains("error"));
	           
	            JavaRDD<ApacheAccessLog> accessLogs0 =
	            		lines0.map(ApacheAccessLog::parseFromErrorLogLine).cache();
	            
	            JavaPairRDD<String, Integer> ones0 = accessLogs0.mapToPair(log -> new Tuple2<>(log.getErrorMsg(), 1))
	            		.reduceByKey((i1, i2) -> i1 + i2);
	            
	            List<Tuple2<String,Integer>> counts0 = ones0.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(5);
	            
	             JavaRDD<String> mainRDD1 = sc.textFile("../LogAnalysis/input/"+args[2]);
		            JavaRDD<String> lines1 = mainRDD1
		            		.filter(s -> s.toLowerCase().contains("error"));
		           
		            JavaRDD<ApacheAccessLog> accessLogs1 = lines1.map(ApacheAccessLog::parseFromErrorLogLine).cache();
		            JavaPairRDD<String, Integer> ones1 = accessLogs1.mapToPair(log -> new Tuple2<>(log.getErrorMsg(), 1))
		            		.reduceByKey((i1, i2) -> i1 + i2);
		            List<Tuple2<String,Integer>> counts1 = ones1.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(5);

		            System.out.println("* Q6: 5 most frequent error messages");
	                System.out.print("   + "+args[1]+" :\n");
		             for (Tuple2<?,?> tuple : counts0) {
		            	 String s1="\t  - ("+tuple._2().toString()+", ";
		            	 String s2= "'"+tuple._1().toString()+"')";
		            	 System.out.println(s1+s2);
		             }
		            System.out.print("   + "+args[2]+" :\n");
		             for (Tuple2<?,?> tuple : counts1) {
		            	 String s1="\t  - ("+tuple._2().toString()+", ";
		            	 String s2= "'"+tuple._1().toString()+"')";
		            	 System.out.println(s1+s2);
		             }

	    		
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
