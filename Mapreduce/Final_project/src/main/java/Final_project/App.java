package Final_project;
import java.io.File;

import org.apache.commons.io.FileUtils;
//
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class App 
{
	public static void main( String[] args ) throws Exception {
			JobConf conf = new JobConf(App.class);
			conf.setJobName("App");
	
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);
	
			conf.setMapperClass(Map.class);
			//conf.setCombinerClass(Reduce.class);
			conf.setReducerClass(Reduce.class);
	
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileUtils.deleteDirectory(new File(args[1]));
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}

//import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;   
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//public class App extends Configured implements Tool {
//
// public int run(String[] args) throws Exception {
//
//    JobControl jobControl = new JobControl("jobChain"); 
//    Configuration conf1 = getConf();
//
//    Job job1 = Job.getInstance(conf1);  
//    job1.setJarByClass(App.class);
//    job1.setJobName("Word Combined");
//
//    FileInputFormat.setInputPaths(job1, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp2"));
//    
//    job1.setMapperClass(Map.class);
//    job1.setMapOutputKeyClass(Text.class);
//    job1.setMapOutputValueClass(IntWritable.class);
//    job1.setReducerClass(Reduce.class);
////    job1.setCombinerClass(SumReducer.class);
//
//    job1.setOutputKeyClass(Text.class);
//    job1.setOutputValueClass(IntWritable.class);
//
//    ControlledJob controlledJob1 = new ControlledJob(conf1);
//    controlledJob1.setJob(job1);
//
//    jobControl.addJob(controlledJob1);
////    Configuration conf2 = getConf();
//
////    Job job2 = Job.getInstance(conf2);
////    job2.setJarByClass(App.class);
////    job2.setJobName("Word Invert");
////
////    FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
////    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
//
////    job2.setMapperClass(WordMapper2.class);
////    job2.setReducerClass(SumReducer2.class);
////    job2.setCombinerClass(SumReducer2.class);
////
////    job2.setOutputKeyClass(IntWritable.class);
////    job2.setOutputValueClass(Text.class);
////    job2.setInputFormatClass(KeyValueTextInputFormat.class);
////
////    job2.setSortComparatorClass(IntComparator.class);
////    ControlledJob controlledJob2 = new ControlledJob(conf2);
////    controlledJob2.setJob(job2);
////
////    // make job2 dependent on job1
////    controlledJob2.addDependingJob(controlledJob1); 
////    // add the job to the job control
//    jobControl.addJob(controlledJob1);
//    Thread jobControlThread = new Thread(jobControl);
//    jobControlThread.start();
////
//while (!jobControl.allFinished()) {
//    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
//    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//try {
//    Thread.sleep(1000);
//    } catch (Exception e) {
//
//    }
//
//  } 
//   System.exit(0);  
//   return (job1.waitForCompletion(true) ? 0 : 1);   
//	
//  } 
//  public static void main(String[] args) throws Exception { 
//  int exitCode = ToolRunner.run(new App(), args);  
//  System.exit(exitCode);
//  }
//}
