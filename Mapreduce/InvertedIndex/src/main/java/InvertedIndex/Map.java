package InvertedIndex;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, Text> {
	private Text blockadrr = new Text();
	private Text crimetype = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException,InterruptedException
	{
		
		 String line = value.toString();
//		 System.out.println(line);
		    String IP = line.split(",")[3];
		    String urls = line.split(",")[7];
		    
//		    System.out.println(urls);
		    blockadrr.set(IP);
		    crimetype.set(urls);
		   
		    context.write(blockadrr, crimetype);
	}

}
