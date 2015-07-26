package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Text, Text, Text, Text> {
	
	/**
	 * Input to Mapper1 is of the form:
	 * parent_url	child_url1;child_url2;child_url3;.....
	 * 
	 * Output of Mapper1 is of the form:
	 * child_url1	parent_url
	 * child_url2	parent_url
	 * child_url3	parent_url
	 * ..
	 * parent_url	PARENT
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("[DEBUG] Mapper1 : "+key);
		Text parent = new Text(key.toString());
		String[] children = value.toString().split("\\^");		
		for (String child_url: children) {
			Text child = new Text(child_url);
			context.write(child, parent);
		}
		Text parent_indicator = new Text("PARENT");
		if ((parent.toString().trim().length() > 0) && parent.toString().startsWith("http"))
			context.write(parent, parent_indicator);
	}

}
