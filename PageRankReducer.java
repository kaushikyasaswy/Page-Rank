package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double total_value = (double)0.0;
		String value_str;
		String children_string = "";
		for (Text value : values) {
			value_str = value.toString();
			if (value_str.trim().length() == 0) {
				continue;
			}
			else if (value_str.contains("^")) {
				children_string = value_str;
				continue;
			}
			else {
				total_value += Double.parseDouble(value_str);
			}
		}
		total_value = (double)(0.15 + 0.85 * (total_value));
		context.write(new Text(key+"::::"+total_value), new Text(children_string));
	}
}
