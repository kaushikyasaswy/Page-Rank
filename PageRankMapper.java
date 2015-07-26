package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	/**
	 * Input to PageRankMapper is of the form:
	 * parent_url1:initial_rank	child_url1:flag;child_url2:flag;...
	 * 
	 * Output of PageRankMapper is of the form:
	 * 
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] split_array = key.toString().split("::::");
		String parent = split_array[0];
		double parent_rank = Double.parseDouble(split_array[1]);
		String children_string = value.toString();
		// If no children are present for the page
		if (!children_string.contains("^")) {
			context.write(new Text(parent), new Text(split_array[1]));
			return;
		}
		double child_rank;
		String[] children = children_string.split("\\^");
		int num_of_children = 0;
		for (String child : children) {
			try {
				if (child.split("::::")[1].equals("0")) {
					continue;
				}
				num_of_children++;
			}
			// If child is not in the form child:flag
			catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("[ERROR] Child format error : "+key.toString()+" : "+value.toString());
			}
		}
		child_rank = (num_of_children == 0) ? (double)0.15 : (double)parent_rank/num_of_children;
		for (String child : children) {
			if (child.split("::::")[1].equals("0")) {
				continue;
			} 
			else {
				context.write(new Text(child.split("::::")[0]), new Text(Double.toString(child_rank)));
			}
		}
		context.write(new Text(parent), value);
	}

}
