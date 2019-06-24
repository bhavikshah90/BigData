//package LocationFilter;
//
//import java.io.File;
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
////import com.apache.hadoop.design.summarization.blog.ConfigurationFactory;
////import com.apache.hadoop.design
//
//public class App {
//public static void main(String[] args) throws Exception {
///*
//* I have used my local path in windows change the path as per your
//* local machine
//*/
//args = new String[] {"/home/bhavik/Desktop/bigdata/Crimes.csv",
//		"/home/bhavik/eclipse-workspace/LocationFilter/countyear","/home/bhavik/eclipse-workspace/LocationFilter/hotlist" };
////  countyear RESIDENCE
///* delete the output directory before running the job */
//FileUtils.deleteDirectory(new File(args[1]));
//
//System.setProperty("hadoop.home.dir", "Replace this string with hadoop home directory location");
//if (args.length != 3) {
//System.err.println("Please specify the input and output path");
//System.exit(-1);
//}
//	Configuration conf = new Configuration();
//	conf.set("bloom_filter_file_location",args[2]);
//	Job job = Job.getInstance(conf);
//	job.setJarByClass(App.class);
//	job.setJobName("Bloom_Filter_Department");
//	FileInputFormat.addInputPath(job, new Path(args[0]));
//	FileOutputFormat.setOutputPath(job, new Path(args[1]));
//	job.setMapperClass(Mapp.class);
//	job.setOutputKeyClass(Text.class);
//	job.setOutputValueClass(NullWritable.class);
//	System.exit(job.waitForCompletion(true) ? 0 : 1);
//}
//}