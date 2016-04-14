package labs.photocountry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Hashtable;

public class UserCountryReducer extends Reducer<Text, Text, Text, Text> {
    private Hashtable<String, Integer> countryTable = new Hashtable<String, Integer>();
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // create a map to remember the owner frequency
        // keyed on owner id
        String result = "";
        for(Text value: values){
            Integer currentcount = countryTable.get(value.toString());
            if(currentcount!= null){
                countryTable.put(value.toString(),++currentcount);
            }else{
                countryTable.put(value.toString(),1);
            }
        }
        for(String currentKey : countryTable.keySet()){
            result += "\t"+currentKey+": "+countryTable.get(currentKey);
        }
        context.write(key,new Text(result));
    }
}
