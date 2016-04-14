package labs.usertag;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input record format
 * dog -> {48889082718@N01=2}
 * dog -> {3423249@N01=1}
 *
 * Output for the above input key valueList
 * dog -> 48889082718@N01=2,3423249@N01=1,
 * 
 * @author Rafael Possas
 *
 */
public class TagReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// create a map to remember the owner frequency
		// keyed on owner id
		String result = "";
		String separator = "";
		for(Text value: values){
			if(!result.equals("") && separator.equals(""))
				separator = ",";
			result += separator+ value.toString();
		}
		context.write(key,new Text(result));
	}
}
