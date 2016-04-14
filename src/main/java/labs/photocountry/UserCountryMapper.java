package labs.photocountry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.Hashtable;


public class UserCountryMapper extends Mapper<Object, Text, Text, Text> {
    private Hashtable<String, String> placeTable = new Hashtable<String, String>();
    private Text keyOut = new Text(), valueOut = new Text();

    public void setPlaceTable(Hashtable<String,String> place){
        placeTable = place;
    }

    public void putPlaceTable(String line){
        String tokens[] = line.split("\t");
        placeTable.put(tokens[0], tokens[1]); // use full place.txt index is 6, other wise it is 1.
    }

    public void setup(Mapper.Context context) throws IOException{
        Path[] cacheFiles = context.getLocalCacheFiles();
        BufferedReader placeReader = null;
        if (cacheFiles != null && cacheFiles.length > 0) {
            String line;

            try {
                //String filename = cacheFiles[0].toString();
                String filename = System.getProperty("user.dir")+"/tmpfilter/part-m-00000";
                FileInputStream fis = new FileInputStream(filename);
                placeReader = new BufferedReader(new InputStreamReader(fis));
                //placeReader = new BufferedReader(new FileReader(filename));
                while ((line = placeReader.readLine()) != null) {
                    putPlaceTable(line);
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                if(placeReader != null)
                    placeReader.close();
            }
        }
    }

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 6){ // a not complete record with all data
            return; // don't emit anything
        }
        String country = placeTable.get(dataArray[4]);
        if(country!=null){
            context.write(new Text(dataArray[1]),new Text(country));
        }

    }
}
