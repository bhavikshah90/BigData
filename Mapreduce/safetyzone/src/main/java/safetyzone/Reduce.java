//package safetyzone;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.Reporter;
//
//public class Reduce extends MapReduceBase implements Reducer {
//
//	public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
//		// TODO Auto-generated method stub
//		try {
//		int sum = 0;
//		HashMap map = new HashMap();
//        while (values.hasNext()) {
//        	//System.out.println(values.next().toString()); 
//        	if (!map.containsKey(values.next().toString()))
//        	{
//        		System.out.println("inside");
//        		sum = 1;
//        		map.put(values.toString(),sum );
//        		
//        	}
//                       sum += 1;
//        }
//        System.out.println("h");
//        output.collect(key, new IntWritable(sum));
//	}
//		catch(Exception e) {
//			System.out.println(e);
//		}
//		
//	}
//}
////