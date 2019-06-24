//package LocationFilter;
//
//import java.io.DataInputStream;
//import java.io.FileInputStream;
//import java.io.IOException;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.util.bloom.BloomFilter;
//import org.apache.hadoop.util.bloom.Key;
//public class Mapp extends Mapper<Object, Text, Text, NullWritable> {
//	
//	private BloomFilter filter = new BloomFilter();
//	
//	protected void setup(Context context) throws IOException, InterruptedException {
//	DataInputStream dataInputStream = new DataInputStream(
//	new FileInputStream(context.getConfiguration().get("bloom_filter_file_location")));
////	System.out.println(context.getConfiguration().get("bloom_filter_file_location"));
//	filter.readFields(dataInputStream);
//	
//	
//	dataInputStream.close();
//	}
//	
//	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//	String data = value.toString();
//	String[] field = data.split(",");
//	String department = null;
//	if (field[7].length() > 0) {
//	department = field[7];
//	if (filter.membershipTest(new Key(department.getBytes()))) {
//	context.write(value, NullWritable.get());
//	}
//	}
//}
//}