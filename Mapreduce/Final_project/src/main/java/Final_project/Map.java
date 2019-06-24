package Final_project;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements Mapper{
	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
	public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
//  
		   String line = value.toString();
//		   
		   if(line.toLowerCase().contains("location")) {
				 return;
			 }
		   String IP = line.split(",")[5].trim();
		   System.out.println(IP);
//		  
		   word.set(IP);
		   output.collect(word, one);
          
	}
}
