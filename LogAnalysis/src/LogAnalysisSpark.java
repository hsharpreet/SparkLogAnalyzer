

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

		int ques = 0;
		String inputPath1 ="";
		String inputPath2 = "";
		
	    if (args.length < 1) {
	      System.err.println("Usage: LogAnalysisSpark <file>");
	      System.out.println("Arguments not provided properly");
	      System.exit(1);
	    }else if(args.length>2){
	    	ques = Integer.parseInt(args[0]);
	    	inputPath1 = args[1];
	    	inputPath2 = args[2];
	    	
	    	SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
	        sc = new JavaSparkContext(conf);
	        
	        SparkSession spark = SparkSession.builder().appName("LogAnalysisSpark").getOrCreate();
	    	
	    	if(ques==1){
	        	
	        	JavaRDD<String> mainRDD1 = sc.textFile(inputPath1);
	        	JavaRDD<String> mainRDD2 = sc.textFile(inputPath2);
	        	
	            // For printing result on console
	        	long mainRDD1count = mainRDD1.count();
	        	long mainRDD2count = mainRDD2.count();
	                System.out.println("* Q1: line counts");
	                System.out.println("   + "+args[1]+": "+mainRDD1count);
	                System.out.println("   + "+args[2]+": "+mainRDD2count);
	    		
	    	}else if(ques==2){
	    		String user="achille";
	    		JavaRDD<String> mainRDD1 = sc.textFile(inputPath1);
	            JavaRDD<String> lines0 = mainRDD1
	            		.filter(s -> s.contains("Starting"))
	            		.filter(s -> s.contains("Session"))
	            		.filter(s -> s.contains(user));
	        	
	            long ones0 = lines0.count();
	            
	        	JavaRDD<String> mainRDD2 = sc.textFile(inputPath2);
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
	    		JavaRDD<String> mainRDD0 = sc.textFile(inputPath1);
	            JavaRDD<String> lines0 = mainRDD0
	            		.filter(s -> s.contains("Starting Session"))
	            		.filter(s -> s.contains("of user"));
	           
	            JavaRDD<ApacheAccessLog> accessLogs0 = lines0.map(ApacheAccessLog::parseFromLogLine).cache();
	            
	            JavaPairRDD<String, Integer> ones0 = accessLogs0.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
	            JavaPairRDD<String, Integer> counts0 = ones0.reduceByKey((i1, i2) -> i1 + i2);
	            output1 = counts0.collect();
	                
	             JavaRDD<String> mainRDD2 = sc.textFile(inputPath2);
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
		            	 users.add(tuple._1().toString());
		             }
		             System.out.println(users);
	             
		            System.out.print("   + "+args[2]+" :");
	                users.clear();
		             for (Tuple2<?,?> tuple : output2) {
		            	 users.add(tuple._1().toString());
		             }
		             System.out.println(users);

	    		
	    	}else if(ques==4 || ques==7 || ques==8){
	    		JavaRDD<String> mainRDD0 = sc.textFile(inputPath1);
	            JavaRDD<String> lines0 = mainRDD0
	            		.filter(s -> s.contains("Starting Session"))
	            		.filter(s -> s.contains("of user"));
	           
	            JavaRDD<ApacheAccessLog> accessLogs0 = lines0.map(ApacheAccessLog::parseFromLogLine).cache();
	            
	            JavaPairRDD<String, Integer> ones0 = accessLogs0.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
	            
	                
	             JavaRDD<String> mainRDD2 = sc.textFile(inputPath2);
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
			            	 String u="'"+tuple._1().toString()+"', ";
			            	 String c= tuple._2().toString();
			            	 users.add("("+u+c+")");
			             }
			             System.out.println(users);
		             
			            System.out.print("   + "+args[2]+" :");
		                users.clear();
			             for (Tuple2<?,?> tuple : output2) {
			            	 String u="'"+tuple._1().toString()+"', ";
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
			            	 users.add("'"+tuple._1().toString()+"'");
			             }
			             System.out.println(users);
			            
		            }else if(ques==8){
		            	
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
		                	String u="'"+tuple._1().toString()+"', ";
			            	String c= "'"+tuple._2().toString()+"'";
		                	users.add("("+u+c+")");
			             }
			             System.out.println("   + : "+users);
		            }

	    		
	    	}else if(ques==5){
	    		JavaRDD<String> mainRDD1 = sc.textFile(inputPath1);
	            JavaRDD<String> lines0 = mainRDD1
	            		.filter(s -> s.toLowerCase().contains("error"));    	
	            long ones0 = lines0.count();
	            
	        	JavaRDD<String> mainRDD2 = sc.textFile(inputPath2);
	           
	        	JavaRDD<String> lines1 = mainRDD2
	            		.filter(s -> s.toLowerCase().contains("error"));
	
	            long ones1 = lines1.count();
                System.out.println("* Q5: number of errors");
                System.out.println("   + "+args[1] + ": " + ones0);
                System.out.println("   + "+args[2] + ": " + ones1);
	             
	    	}else if(ques==6){
	    		
	    		JavaRDD<String> mainRDD0 = sc.textFile(inputPath1);
	    		JavaRDD<String> lines0 = mainRDD0
	            		.filter(s -> s.toLowerCase().contains("error"));
	           
	            JavaRDD<ApacheAccessLog> accessLogs0 =
	            		lines0.map(ApacheAccessLog::parseFromErrorLogLine).cache();
	            
	            JavaPairRDD<String, Integer> ones0 = accessLogs0.mapToPair(log -> new Tuple2<>(log.getErrorMsg(), 1))
	            		.reduceByKey((i1, i2) -> i1 + i2);
	            
	            List<Tuple2<String,Integer>> counts0 = ones0.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(5);
	            
	             JavaRDD<String> mainRDD1 = sc.textFile(inputPath2);
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

	    		
	    	}else if(ques==1){
	    		JavaRDD<String> mainRDD0 = sc.textFile(inputPath1);
	            JavaRDD<String> lines0 = mainRDD0
	            		.filter(s -> s.contains("Starting Session"))
	            		.filter(s -> s.contains("of user"));
	           
	            JavaRDD<ApacheAccessLog> accessLogs0 =
	            	       lines0.map(ApacheAccessLog::parseFromLogLine).cache();
	            
	            JavaPairRDD<String, Integer> ones0 = accessLogs0.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
	            JavaPairRDD<String, Integer> counts0 = ones0.reduceByKey((i1, i2) -> i1 + i2);
	            output1 = counts0.collect();
	                
	             JavaRDD<String> mainRDD2 = sc.textFile(inputPath2);
		            JavaRDD<String> lines1 = mainRDD2
		            		.filter(s -> s.contains("Starting Session"))
		            		.filter(s -> s.contains("of user"));
		           
		            JavaRDD<ApacheAccessLog> accessLogs1 = lines1.map(ApacheAccessLog::parseFromLogLine).cache();
		            
		            JavaPairRDD<String, Integer> ones1 = accessLogs1.mapToPair(log -> new Tuple2<>(log.getUsername(), 1));
		            JavaPairRDD<String, Integer> counts1 = ones1.reduceByKey((i1, i2) -> i1 + i2);
		            
		            output2 = counts1.collect();

	                System.out.println("   + "+args[1]+" :");
	                System.out.println("\tUser name mapping:");
	                ArrayList<String> users1 = new ArrayList<String>();
	                ArrayList<String> users2 = new ArrayList<String>();
		             for (Tuple2<?,?> tuple : output1) {
		            	 users1.add(tuple._1().toString());
		             }
		             
		            System.out.println("   + "+args[2]+" :");
		            System.out.println("\tUser name mapping:");
		             for (Tuple2<?,?> tuple : output2) {
		            	 users2.add(tuple._1().toString());
		             }

		             users1.sort(new Comparator<String>() {
							@Override
							public int compare(String o1, String o2) {
								return o1.compareTo(o2);
							}
					});
		             users2.sort(new Comparator<String>() {
						@Override
						public int compare(String o1, String o2) {
							return o1.compareTo(o2);
						}
		             });
		             System.out.println(users1);
		             System.out.println(users2);
		             
		             for(int i=0;i<users1.size();i++){
		            	 //users1.get(i).
		             }
	    	}else{
	    		System.out.println("");
	    	}
	    	
	
	      sc.stop();
	      spark.stop();
	    }else{
	    	
	    }
  }
}

 class ApacheAccessLog implements Serializable {
	  private static final Logger logger = Logger.getLogger("Access");

	  private String day;
	  private String date;
	  private String time;
	  private String user;
	  private String system;
	  private String msg1;
	  private String msg2;
	  private String num;
	  private String msg3;
	  private String msg4;
	  private String username;
	  private String errorMsg;
	  

	  private ApacheAccessLog(String day, String date, String time, String user, 
			  String system, String msg1, String msg2, String num, String msg3, 
			  String msg4, String username) {
	    this.day = day;
	    this.date = date;
	    this.time = time;
	    this.user = user;  
	    this.system = system;
	    this.msg1 = msg1;
	    this.msg2 = msg2;
	    this.num = num;
	    this.msg3 = msg3;
	    this.msg4 = msg4;
	    this.username = username;    
	  }
	  
	  

	  //Mar 13 17:14:50 iliad journal: ethtool ioctl error: No such device
	  public ApacheAccessLog(String day, String date, String time, String user, 
			   String errorMsg) {
		  this.day = day;
		    this.date = date;
		    this.time = time;
		    this.user = user;  
		    this.errorMsg = errorMsg;
		}
		
		public String getErrorMsg() {
			return errorMsg;
		}
		
		public void setErrorMsg(String errorMsg) {
			this.errorMsg = errorMsg;
		}
		
		public String getDay() {
			return day;
		}
		
		public void setDay(String day) {
			this.day = day;
		}
		
		public String getDate() {
			return date;
		}
		
		public void setDate(String date) {
			this.date = date;
		}
		
		public String getTime() {
			return time;
		}
		
		public void setTime(String time) {
			this.time = time;
		}
		
		public String getUser() {
			return user;
		}
		
		public void setUser(String user) {
			this.user = user;
		}
		
		public String getSystem() {
			return system;
		}
		
		public void setSystem(String system) {
			this.system = system;
		}
		
		public String getMsg1() {
			return msg1;
		}
		
		public void setMsg1(String msg1) {
			this.msg1 = msg1;
		}
		
		public String getMsg2() {
			return msg2;
		}
		
		public void setMsg2(String msg2) {
			this.msg2 = msg2;
		}
		
		public String getNum() {
			return num;
		}
		
		public void setNum(String num) {
			this.num = num;
		}
		
		public String getMsg3() {
			return msg3;
		}
		
		public void setMsg3(String msg3) {
			this.msg3 = msg3;
		}
		
		public String getMsg4() {
			return msg4;
		}
		
		public void setMsg4(String msg4) {
			this.msg4 = msg4;
		}
		
		public String getUsername() {
			return username.substring(0, username.length()-1);
		}
		
		public void setUsername(String username) {
			this.username = username;
		}



	// Example Apache log line:
	  //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
	  private static final String LOG_ENTRY_PATTERN =
	      // 1:IP  2:client 3:user 4:date time 5:method 6:req 7:proto   8:respcode 9:size
	     // "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
			  //Feb 28 03:30:01 iliad systemd: Started Session 4415 of user achille.
			  //Feb 28 09:27:26 iliad systemd: Starting Session c1 of user hector.
			  //'^"(\S+) (\d{3}) (\S+)" (\S+) (\S+\:) (\S+) (\S+) (\d{3}) (\S+) (\S+) (\S+)'  
	  "(\\S+) (\\d+) (\\d+:\\d+:\\d+) (\\S+) (\\S+\\:) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S*)";
	  
	  private static final String ERROR_LOG_ENTRY_PATTERN = "(\\S+) (\\d+) (\\d+:\\d+:\\d+) (\\S+) (\\S+\\:.*)";
	  private static final Pattern ERROR_PATTERN = Pattern.compile(ERROR_LOG_ENTRY_PATTERN);
	  
	  private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	  public static ApacheAccessLog parseFromLogLine(String logline) {
		  String s = logline.replaceAll("\\s+", " ");
	    Matcher m = PATTERN.matcher(s);
	    if (!m.find()) {
	      System.out.println("Cannot parse logline " + logline);
	      throw new RuntimeException("Error parsing logline");
	    }

	    return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
	        m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10),m.group(11));
	  }
	  
	  public static ApacheAccessLog parseFromErrorLogLine(String logline) {
		  String s = logline.replaceAll("\\s+", " ");
	    Matcher m = ERROR_PATTERN.matcher(s);
	    if (!m.find()) {
	      System.out.println("Cannot parse ERROR logline " + logline);
	      throw new RuntimeException("Error parsing ERROR logline");
	    }

	    return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
	        m.group(5));
	  }
	}
