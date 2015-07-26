package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {

	/**
	 * Input to Reducer2 after global sorting is of the form:
	 * parent_url1	child_url1:flag
	 * parent_url1	child_url2:flag
	 * ..
	 * 
	 * Output of Reducer2 is of the form:
	 * parent_url1:initial_rank	child_url1:flag;child_url2:flag;...
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println("[DEBUG] Reducer2 "+key);
		String value = "";
		for (Text child : values) {
			value += child.toString() + "^";
		}
		context.write(new Text(key+"::::1.0"), new Text(value));
	}
	
}