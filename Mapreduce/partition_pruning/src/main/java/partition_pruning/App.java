package partition_pruning;

/**
 * Hello world!
 *
 */
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File(args[1]));
		if (args.length != 2) {
		System.err.println("Please specify the input and output path");
		System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setJarByClass(App.class);
		job.setJobName("Partioning_Pattern");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(18);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}