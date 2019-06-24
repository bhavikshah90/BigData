package NormalFilter;

/**
 * Hello world!
 *
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class App 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	long start_time = System.currentTimeMillis();
    	Configuration conf = new Configuration();
   	 	conf.set("mapregex", "HOMICIDE");
   	    Job job = Job.getInstance(conf, "Stocks");
   	    job.setJarByClass(App.class);
   	    job.setMapperClass(Mapp.class);
   	    job.setMapOutputKeyClass(NullWritable.class);
   	    job.setMapOutputValueClass(Text.class);
//   	    job.setReducerClass(Reduce.class);
   	    job.setNumReduceTasks(1);
//   	    job.setOutputKeyClass(Text.class);
//   	    job.setOutputValueClass(BooleanWritable.class);
   	    FileInputFormat.addInputPath(job, new Path(args[0]));
   	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
   	    Path outDir = new Path(args[1]);
   		FileSystem fs = FileSystem.get(job.getConfiguration());
   		if(fs.exists(outDir)) {
   			fs.delete(outDir, true);
   		}
   		
   	    System.exit(job.waitForCompletion(true) ? 0 : 1);
   	    System.out.println("Time Taken is :" + (System.currentTimeMillis() - start_time)/ 1000);
    }
}
