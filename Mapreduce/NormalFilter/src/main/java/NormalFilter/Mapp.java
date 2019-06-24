package NormalFilter;


import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class Mapp extends Mapper<Object, Text, NullWritable, Text> {

    private String mapRegex = null;

    public void setup(Context context) throws IOException,
        InterruptedException {
        
    	mapRegex = context.getConfiguration().get("mapregex");
//    	System.out.println(mapRegex);
    } 
    
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
//    	System.out.println(value.toString().split(",")[7]);
    	String location = value.toString().split(",")[5];
        if (location.matches(mapRegex)) {
        	
           context.write(NullWritable.get(), value);
        }
    }
}