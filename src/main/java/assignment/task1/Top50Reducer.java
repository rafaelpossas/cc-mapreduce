package assignment.task1;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by rafaelpossas on 4/20/16.
 */
public class Top50Reducer extends Reducer<Text,Text,Text,Text> {

    private Map<String,Integer> localityCountTable = new HashMap<String, Integer>();
    private Map<String,String> localityTagTable = new HashMap<String, String>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer totalPhotos = 0;
        Map<String,Integer> tagCountTable = new HashMap<String, Integer>();
        for(Text value: values){
            String[] dataArray = value.toString().split("\t");
            totalPhotos+= Integer.parseInt(dataArray[0]);
            String[] tagArray = dataArray[1].split(" ");
            for (String tag: tagArray){
                String[] tagCount = tag.split(";");
                Integer count = tagCountTable.get(tagCount[0]);
                if(count == null){
                    tagCountTable.put(tagCount[0],Integer.parseInt(tagCount[1]));
                }else{
                    Integer totalCount = Integer.parseInt(tagCount[1]) + tagCountTable.get(tagCount[0]);
                    tagCountTable.put(tagCount[0],totalCount);
                }
            }
        }
        localityCountTable.put(key.toString(),totalPhotos);
        tagCountTable = Utils.sortByValues(tagCountTable);
        String result = "";
        int count = 0;
        for (String orderedTag: tagCountTable.keySet()){
            if(count < 10){
                result+= "{"+orderedTag+":"+tagCountTable.get(orderedTag)+"} ";
            }
            count++;
        }
        localityTagTable.put(key.toString(),result);
    }
    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException {
        localityCountTable = Utils.sortByValues(localityCountTable);
        int count = 0;
        for (String locality: localityCountTable.keySet()){
            if(count < 50){
                context.write(new Text(locality),new Text(localityCountTable.get(locality)+"\t"+localityTagTable.get(locality)));
            }
            count++;
        }
    }

}
