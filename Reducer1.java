package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	/**
	 * Input to Reducer1 after global sorting is of the form:
	 * child_url	parent_url1
	 * child_url	parent_url2
	 * child_url	PARENT
	 * ..
	 * 
	 * Output of Reducer1 is of the form:
	 * parent_url1	child_url:flag
	 * parent_url2	child_url:flag
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println("[DEBUG] Reducer1 : "+key);
		String flag = "0"; //flag = 0 indicates the child is a dangling url and flag = 1 indicates the child is a non dangling url
		String child = key.toString();
		ArrayList<String> parents = new ArrayList<String>();
		for (Text parent : values) {
			if (parent.toString().equals("PARENT")) {
				flag = "1";
				continue;
			}
			parents.add(parent.toString());
		}
		for (String out_key : parents) {
			if ((out_key.toString().trim().length() > 0) && out_key.toString().startsWith("http") && (child.toString().trim().length() > 0) && child.toString().startsWith("http"))
				context.write(new Text(out_key), new Text(child+"::::"+flag));
		}
	}

}
