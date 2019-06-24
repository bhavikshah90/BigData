package Binning;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BinningMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private MultipleOutputs<Text, NullWritable> mos = null;
	    
	    protected void setup(Context context){
	        mos = new MultipleOutputs(context);
//	        System.out.println("inside");
//	        System.out.println(mos);
	    }
	    
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	        
        if(key.get()==0){
        return;
    }
        String[] token = value.toString().split(",");
        System.out.println(token);
            //for(String rate: token){
                
                String crime_type = token[5].trim();
                Map map = new HashMap();
                if (!map.containsKey(crime_type)){
                map.put(crime_type, crime_type);
                }
                
                if(map.containsKey(crime_type)){
                    mos.write("bins", value, NullWritable.get(), map.get(crime_type).toString());
                }
//                 
}         
            protected void cleanup(Context context) throws IOException, InterruptedException{
                mos.close();
            }


}