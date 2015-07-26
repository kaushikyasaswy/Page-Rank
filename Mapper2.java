package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Text, Text, Text, Text> {
	
	/**
	 * Pass the output of Reducer1 to Reducer2 directly
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("[DEBUG] Mapper2 "+key);
		context.write(key, value);
	}

}
