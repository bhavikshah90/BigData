package partition_pruning;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, Text> {
	public Text mapperKey=new Text();
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String data = value.toString();
	String[] field = data.split(",");
	if (field[17].trim().length() > 0 && field[17].trim().length() < 5) {
	String departmentName = field[17];
	mapperKey.set(departmentName);
	context.write(mapperKey, value);
	}
}
}