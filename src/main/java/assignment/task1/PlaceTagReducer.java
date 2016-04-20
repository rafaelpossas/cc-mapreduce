package assignment.task1;

import labs.reducesidejoin.TextIntPair;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rafaelpossas on 4/20/16.
 */
public class PlaceTagReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalPhotos = 0;
        for(Text value: values){
            String[] valueArray = value.toString().split("\t");
            String[] tagArray = valueArray[0].split(" ");
            String[] years = valueArray[1].split(" ");
            String[] places = valueArray[2].split(" ");
            totalPhotos += Integer.parseInt(valueArray[3]);
            Map<String,Integer> tagMap = new HashMap<String, Integer>();
            for (String tag: tagArray){
                try{
                    if(!tag.trim().equals("")){
                        String[] tagCount = tag.split(";");
                        if(!ArrayUtils.contains(places,tagCount[0].toLowerCase()) && !ArrayUtils.contains(years,tagCount[0].toLowerCase())){
                            tagMap.put(tagCount[0],Integer.parseInt(tagCount[1]));
                        }

                    }
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
            tagMap = Utils.sortByValues(tagMap);
            String result = "";
            int count = 0;

            for(String tagKey: tagMap.keySet()){
                if(count < 10){
                    result+= tagKey+";"+tagMap.get(tagKey)+" ";
                }else{
                    break;
                }
                count++;
            }
            context.write(new Text(key.toString()),new Text(totalPhotos+"\t"+result.toString()));
        }
    }
}
