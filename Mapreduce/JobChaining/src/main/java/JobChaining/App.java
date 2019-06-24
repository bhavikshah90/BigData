/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package JobChaining;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1,"Amazon Average");
        job1.setJarByClass(App.class);
        job1.setMapperClass(Map1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(BooleanWritable.class);
        
        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        FileUtils.deleteDirectory(new File(args[1]));
		FileUtils.deleteDirectory(new File(args[2]));
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean complete = job1.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"Chaining Sorting");
        
        if(complete){
        job2.setJarByClass(App.class);
        job2.setMapperClass(Map2.class);
        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
        System.exit(job2.waitForCompletion(true) ? 0:1);
        
        }
    }
    
    public static class Map1 extends Mapper<LongWritable, Text, Text, BooleanWritable>{
        
        private Text text = new Text();
        private BooleanWritable arrested = new BooleanWritable();
        
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            
            if(key.get()==0){
            return;
            }
            
            else{
            
            String[] line = value.toString().split(",");
            if (line[8]=="arrest") {
            	return;
            }
            String fbiCode = line[14].trim();
            boolean arrest = Boolean.valueOf(line[8].trim());
            System.out.println(arrest);
            
            text.set(fbiCode);
            arrested.set(arrest);
            
            context.write(text, arrested);
            
            }
        }
    }
    
      public static class Reduce1 extends Reducer<Text, BooleanWritable, Text, FloatWritable>{
        
        private FloatWritable result = new FloatWritable();
        
        @Override
        protected void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException{
           
            float sum = 0;
            int count = 0;
            for(BooleanWritable val:values){
                System.out.println(val.get());
            	if (val.get() == true) {
	                sum += 1;
//	                System.out.println("float");        
            	}
            	count = count+1;
            }
            
            float average = (sum/count)*100;
            
            result.set(average);
            context.write(key,result);
        }
        
    }

    public static class Map2 extends Mapper<LongWritable, Text, FloatWritable, Text>{
        
        public void map(LongWritable key, Text value, Context context){
            
            String[] row =value.toString().split("\\t");
            Text fbi = new Text(row[0]);
            float counting = Float.valueOf(row[1].trim());
            
            try{
                FloatWritable count = new FloatWritable(counting);
                context.write(count, fbi);
            }
            
            catch(Exception e){
                
            }   
        }
    }
    
    
    public static class Reduce2 extends Reducer<FloatWritable, Text, Text, FloatWritable>{
    
        public void reduce(FloatWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
        
        for(Text val : value){
           
        context.write(val,key);
     }        
    }
   }
}
