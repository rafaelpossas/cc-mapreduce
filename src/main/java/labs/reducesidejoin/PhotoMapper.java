package labs.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * The mapper to read photo
 * 
 * using n05.txt as the photo table 
 * n05.txt's format is:
 * photo_id \t owner \t tags \t date_taken\t place_id \t accuracy
 * 
 * output: place_id, user \t date
 * @author ying Zhou
 *
 */
public class PhotoMapper extends Mapper<Object, Text, TextIntPair, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >=5)
			context.write(new TextIntPair(dataArray[4],1), new Text(dataArray[1] + "\t" + dataArray[3]));
		
	}

}
