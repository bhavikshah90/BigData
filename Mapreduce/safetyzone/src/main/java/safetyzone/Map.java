//package safetyzone;
//
//import java.io.IOException;
//import java.util.StringTokenizer;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//
//public class Map extends MapReduceBase implements Mapper{//<LongWritable,  /*Input key Type */
////Text,                   /*Input value Type*/
////Text,                   /*Output key Type*/
////IntWritable>{
//	private  Text one = new Text();
//    private Text word = new Text();
//    
//	public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
////    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
////    {
//		//
//		   String line = value.toString();
//		   
////		   System.out.println(line);
//		   if(line.toLowerCase().contains("location")) {
//				 return;
//			 }
//		   String day = line.split(",")[2].split(" ")[0];
////		   System.out.println(day);
//		   String block = line.split(",")[5];
////		   System.out.println(block);
////		  	
//		   one.set(block);
//		   word.set(day);
//		   output.collect(word,one);
//          
//	}
//}
//
////import java.io.IOException;
////
////import org.apache.hadoop.io.IntWritable;
////import org.apache.hadoop.io.Text;
////import org.apache.hadoop.mapred.OutputCollector;
////import org.apache.hadoop.mapreduce.Mapper;
////public class Map extends Mapper< Text, Text, Text, IntWritable> {
////	private final static IntWritable one = new IntWritable(1);
////	private Text word = new Text();
////	
////    public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
////    	 String line = value.toString();
////	   System.out.println(line);
////	   String IP = line.split(",")[5];
//////	  
////	   word.set(IP);
////	   output.collect(word, one);
////}
////	
////}
////		
//	
//
