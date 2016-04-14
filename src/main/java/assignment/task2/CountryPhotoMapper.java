package assignment.task2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rafaelpossas on 14/04/16.
 */
public class CountryPhotoMapper extends Mapper<Object, Text, Text, Text> {
    private Map<String, String> placeMap = new HashMap<String, String>();
    private Text keyOut = new Text(), valueOut = new Text();
    private BufferedReader placeReader;

    public void putPlaceTable(String line){
        String tokens[] = line.split("\t");
        placeMap.put(tokens[0], tokens[1]+"\t"+tokens[2]+"\t"+tokens[3]); // use full place.txt index is 6, other wise it is 1.

    }
    // get the distributed file and parse it
    public void setup(Context context)
            throws IOException, InterruptedException{
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            String line;
            try {
                //placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                String filename = System.getProperty("user.dir")+"/place-type-filter/part-m-00000";
                FileInputStream fis = new FileInputStream(filename);
                placeReader = new BufferedReader(new InputStreamReader(fis));
                while ((line = placeReader.readLine()) != null) {
                    putPlaceTable(line);
                }
            }
            finally {
                if(placeReader!=null)
                    placeReader.close();
            }
        }

    }

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 6){ // a not complete record with all data
            return; // don't emit anything
        }

        if(placeMap.containsKey(dataArray[4])){
            String[] location = placeMap.get(dataArray[4]).split("\t");
            String placeType = location[0];
            String country = location[1];
            String url = location[2];
            if(placeType.equals("7") || placeType.equals("22")){
                keyOut.set(country);
                //keyOut.set(dataArray[4]);
                valueOut.set(dataArray[1]+"\t"+dataArray[2]);
                context.write(keyOut,valueOut);
            }

        }
    }
}
