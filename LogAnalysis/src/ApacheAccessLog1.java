import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents an Apache access log line.
 * See http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog implements Serializable {
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