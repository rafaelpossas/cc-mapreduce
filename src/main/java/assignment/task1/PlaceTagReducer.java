package assignment.task1;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Utils;

import java.io.IOException;
import java.util.*;

public class PlaceTagReducer extends Reducer<Text,Text,Text,Text> {
    private Map<String,Integer> localityTable = new HashMap<String, Integer>();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text currentValue: values){
            String[] valueArray = currentValue.toString().split("\t");
            String tags = valueArray[0];
            String years = valueArray[1];
            String places = valueArray[2];
            int totalPhotos = Integer.parseInt(valueArray[3]);

            localityTable.put(key.toString()+"\t"+tags+"\t"+years
                    +"\t"+places,totalPhotos);
        }



    }
    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
        Map<String, Integer> sortedMap = Utils.sortByValues(localityTable);
        Map<String, Integer> tagTable;
        int counter = 0;
        for (String key : sortedMap.keySet()) {
            tagTable = new HashMap<String, Integer>();
            if (counter++ == 50) {
                break;
            }
            String[] dataArray = key.split("\t");
            String[] tags = dataArray[1].split(" ");
            String[] years = dataArray[2].split(" ");
            String[] locality = dataArray[3].split(" ");
            String result = "";

            for (String tag: tags){

                if(!ArrayUtils.contains(locality,tag.toLowerCase()) && !ArrayUtils.contains(years,tag.toLowerCase())){
                    Integer count = tagTable.get(tag);
                    if(count!=null){
                        tagTable.put(tag.toString(),++count);
                    }else{
                        tagTable.put(tag.toString(),1);
                    }
                }
            }
            Map<String, Integer> sortedTags = Utils.sortByValues(tagTable);
            int count = 0;
            for(String current_key: sortedTags.keySet()){
                if(count < 10){
                    result+= current_key+":"+tagTable.get(current_key)+" ";
                    count++;
                }else{
                    break;
                }

            }
            context.write(new Text(dataArray[0]+"\t"+sortedMap.get(key)),new Text(result));
        }
    }

}
