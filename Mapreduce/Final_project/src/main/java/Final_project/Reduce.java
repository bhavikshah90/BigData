package Final_project;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer {

	public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		int sum = 0;
        while (values.hasNext()) {
                       sum += ((IntWritable) values.next()).get();
        }
        output.collect(key, new IntWritable(sum));
	}
}
//
//import java.io.IOException;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapreduce.Reducer;
//
//public class Reduce extends Reducer<Text, Text, Text, IntWritable> {
//	public void reduce(Text key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
//	// TODO Auto-generated method stub
//	int sum = 0;
//    while (values.hasNext()) {
//                   sum += ((IntWritable) values.next()).get();
//    }
//    output.collect(key, new IntWritable(sum));
//}
//}