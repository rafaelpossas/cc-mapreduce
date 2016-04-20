package assignment.task1;

import labs.reducesidejoin.TextIntPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;

/*
 * Uses the file in distributed cache created by PlaceTypeMapper and joins with the Photos files
 *
 * Distributed Cache File Structure:
 * place-id  locality country place-url
 *
 * Photos File Structure
 * photo-id owner tags date-taken place-id accuracy
 *
 * Output
 * Locality tags \t date \t URL
 */
public class PlaceTagMapper extends Mapper<Object, Text, Text, Text> {

    private Map<String, String> placeMap = new HashMap<String, String>();
    private Text keyOut;
    private Text valueOut;
    private BufferedReader placeReader;

    public void putPlaceTable(String line){
        String tokens[] = line.split("\t");
        placeMap.put(tokens[0], tokens[1]+"\t"+tokens[2]); // use full place.txt index is 6, other wise it is 1.

    }
    // get the distributed file and parse it
    public void setup(Context context)
            throws IOException, InterruptedException{
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            String line;
            try {
                placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));

            } catch(Exception e){
                if(placeReader == null){
                    String filename = System.getProperty("user.dir")+"/tmp/places/part-m-00000";
                    FileInputStream fis = new FileInputStream(filename);
                    placeReader = new BufferedReader(new InputStreamReader(fis));
                }

            } finally {
                if(placeReader!=null){
                    while ((line = placeReader.readLine()) != null) {
                        putPlaceTable(line);
                    }
                    placeReader.close();
                }

            }
        }

    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 6){ // a not complete record with all data
            return; // don't emit anything
        }

        if(placeMap.containsKey(dataArray[4])){
            String[] location = placeMap.get(dataArray[4]).split("\t");
            String locality = location[0];
            String places = location[1];
            keyOut = new Text(locality);
            //keyOut.set(dataArray[4]);
            valueOut = new Text(dataArray[2]+"\t"+dataArray[3]+"\t"+places);
            context.write(keyOut,valueOut);
        }

    }
}
