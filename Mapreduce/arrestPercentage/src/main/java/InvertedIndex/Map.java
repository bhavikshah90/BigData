package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, BooleanWritable> {
	private BooleanWritable arrest1 = new BooleanWritable();
	private Text crime1 = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException,InterruptedException
	{
		
		 String line = value.toString();
		 if(line.toLowerCase().contains("location")) {
			 return;
		 }
//		 System.out.println(line);
		    String arrest = line.split(",")[8];
		    String crime = line.split(",")[5];
		    boolean arrestBool = Boolean.parseBoolean(arrest);
		    arrest1.set(arrestBool);
		    crime1.set(crime);
		    
		    context.write(crime1,arrest1);
	}

}
