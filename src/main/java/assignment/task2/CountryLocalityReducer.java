package assignment.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rafaelpossas on 4/15/16.
 */
public class CountryLocalityReducer extends Reducer<Text,Text,Text,Text> {

    private Map<String,HashMap<String,Integer>> countryTable = new HashMap<String, HashMap<String,Integer>>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        HashMap<String,Integer> localityMap = countryTable.get(key.toString());
        try{
            for (Text value: values){
                String currentLocality = value.toString().split(";")[1];
                System.out.println(key.toString()+"\t"+value.toString()+"\n");
                if(localityMap == null){
                    localityMap = new HashMap<String, Integer>();
                    localityMap.put(currentLocality,1);
                    countryTable.put(key.toString(),localityMap);
                }else{
                    if(localityMap.get(currentLocality)== null){
                        localityMap.put(currentLocality.toString(),1);
                    }else{
                        int currentCount = localityMap.get(currentLocality);
                        localityMap.put(currentLocality,++currentCount);
                    }

                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }


    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String key: countryTable.keySet()){
            System.out.println("\n"+key+"\n");
            HashMap<String,Integer> localityMap = countryTable.get(key.toString());
            String result = "";
            for (String localKey: localityMap.keySet()){
                result += localKey+":"+localityMap.get(localKey)+" ";
            }
            context.write(new Text(key),new Text(result));
        }
    }
}
