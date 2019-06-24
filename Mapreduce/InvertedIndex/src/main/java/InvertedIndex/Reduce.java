package InvertedIndex;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class Reduce extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();
	  @Override
	  public void reduce(Text key, Iterable<Text> values, Context context)
	          throws IOException, InterruptedException {
		  try {
		  HashMap m=new HashMap();
		  int count=0;
		  StringBuilder sb = new StringBuilder();
		  boolean first = true;
		  
		  for(Text crime_Type : values) {
			  String str=crime_Type.toString();
			  if(m!=null &&m.get(str)!=null){
				  count+= 1;
				  m.put(str, ++count);
				  }else{
				  /*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
				  m.put(str, 1);
				  }
//			  if(first) {
//				  first=false;
//			  }else {
//				  sb.append(",");
//				  
//			  }
//			  sb.append(crime_Type.toString());
		  }
		  
//		  result.set(sb.toString());
//		  context.write(key,result);
		  context.write(key, new Text(m.toString()));
		  }
		  catch(Exception e) {
			  System.out.println(e);
		  }
	  }
}
