package labs.reducesidejoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class JoinReducer extends  Reducer<TextIntPair, Text, Text, Text> {

	public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//check if the key is coming from the place table
		Iterator<Text> valuesItr = values.iterator();
		if (key.getOrder().get() == 0){// the key is from the place table
			String placeName = valuesItr.next().toString();
			while (valuesItr.hasNext()){
				Text value = new Text(valuesItr.next().toString() + "\t" + placeName);
				context.write(key.getKey(), value);
			}
		}else{ // the key is not from the place table, but the photo table
			while(valuesItr.hasNext()){
				context.write(key.getKey(), new Text(valuesItr.next().toString() + "\t" + "NULL"));
			}
		}	
	}
}
