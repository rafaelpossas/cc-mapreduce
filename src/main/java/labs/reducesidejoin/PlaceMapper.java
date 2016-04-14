package labs.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The mapper to read place data
 * 
 * For testing, using place.txt as the photo table 
 * place.txt has the format:
 * place_id \t woeid \t longitude \t latitude \t place name \t place_type_id \t place_url
 * 
 * @author Ying  Zhou
 *
 */
public class PlaceMapper extends  Mapper<Object, Text, TextIntPair, Text> {
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >=6)
		context.write(new TextIntPair(dataArray[0],0), new Text(dataArray[4]));
	}
}
