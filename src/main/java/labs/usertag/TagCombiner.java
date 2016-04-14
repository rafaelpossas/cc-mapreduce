package labs.usertag;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * input record format
 * dog -> 48889082718@N01,48889082718@N01
 * francis -> 48889082718@N01
 *
 * output key value pairs for the above input
 * dog -> 48889082718@N01=2
 * francis -> 48889082718@N01=1
 *
 * @author Rafael Possas
 *
 */
public class TagCombiner extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> ownerFrequency = new HashMap<String,Integer>();

        for (Text text: values){
            String ownerId = text.toString();
            if (ownerFrequency.containsKey(ownerId)){
                ownerFrequency.put(ownerId, ownerFrequency.get(ownerId) +1);
            }else{
                ownerFrequency.put(ownerId, 1);
            }
        }
        String owners;
        for(String currentKey: ownerFrequency.keySet()){
            owners = currentKey+'='+ownerFrequency.get(currentKey).toString();
            context.write(key,new Text(owners));
        }




    }
}
