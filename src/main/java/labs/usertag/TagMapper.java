package labs.usertag;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * input record format
 * 2048252769	48889082718@N01	dog francis lab	2007-11-19 17:49:49	RRBihiubApl0OjTtWA	16
 * 
 * output key value pairs for the above input
 * dog -> 48889082718@N01
 * francis -> 48889082718@N01
 * 
 * @author Rafael Possas
 *
 */
public class TagMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text word = new Text(),owner = new Text();
	
	// a mechanism to filter out non ascii tags
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	
	public void map(LongWritable key, Text value, Context context
	) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 5){ //  record with incomplete data
			return; // don't emit anything
		}
		String tagString = dataArray[2];
		String ownerString = dataArray[1];
		if (tagString.length() > 0){
			String[] tagArray = tagString.split(" ");
			for(String tag: tagArray) {
				if (asciiEncoder.canEncode(tag)){
					word.set(tag);
					owner.set(ownerString);
					context.write(word, owner);
				}
			}
		}
	}
}
