package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class Reduce extends Reducer<Text, BooleanWritable, Text, Text> {
	private Text result = new Text();
	  @Override
	  public void reduce(Text key, Iterable<BooleanWritable> values, Context context)
	          throws IOException, InterruptedException {
//		  System.out.println("here reducer;");
		   float true_count = 0;
		   float false_count = 0;
		   float count = 0;
		   float percentage_solved = 0;
		  boolean first = true;
		  
		  for(BooleanWritable arrest : values) {
			  count++;
//			  System.out.println(arrest.toString());
			  if(arrest.get() == true) {
//				  System.out.println(arrest.toString());
				  true_count++ ;  
//				  System.out.println(true_count);
			  }else {
//				  sb.append(",");
				  false_count++;
//				  System.out.println(false_count);
			  }
//			  sb.append(crime.toString());
		  }
//		  System.out.println(true_count);
		  
//		  System.out.println(true_count/count);
		  percentage_solved = (true_count/count)*100;
//		  System.out.println(percentage_solved);
		  result.set(Float.toString(percentage_solved));
		  context.write(key,result);
}
}
