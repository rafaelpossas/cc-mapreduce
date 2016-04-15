package assignment.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*
 * Receives the data from CountryPhotoMapper in the following format:
 *
 * country owner locality
 *
 * Outputs unique combination of:
 * country user|locality
 *
 */
public class CountryPhotoCombiner extends Reducer<Text,Text,Text,Text> {
    private Map<String,String> localityTable = new HashMap<String, String>();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        for (Text value: values){
            String currentUserLocality = value.toString().split("\t")[0]+";"+value.toString().split("\t")[1]; // Key = Country|UserID
            if(localityTable.get(currentUserLocality) == null){
                localityTable.put(currentUserLocality,key.toString());
                context.write(new Text(key),new Text(currentUserLocality));
            }


        }
    }

}
