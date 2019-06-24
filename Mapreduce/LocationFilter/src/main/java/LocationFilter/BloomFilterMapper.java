package LocationFilter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;

public class BloomFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private BloomFilter<Location> friends;
	Funnel<Location> p = new Funnel<Location>() {
		public void funnel(Location from, Sink into) {
			into.putString(from.district, Charsets.UTF_8);
		}
	};
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		this.friends = BloomFilter.create(p, 500, 0.1);
		Location p1 = new Location("20");
		Location p2 = new Location("4");
		Location p3 = new Location("2");
		ArrayList<Location> friendList = new ArrayList<Location>();
		friendList.add(p1);
		friendList.add(p2);
		friendList.add(p3);
		for (Location p : friendList) {
			friends.put(p);
		}

	}
			
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		Location p = new Location(values[11]);

		if (friends.mightContain(p)) {
			context.write(value, NullWritable.get());
		}
	}
}
