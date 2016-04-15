package assignment.task1;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Utils;

import java.io.IOException;
import java.util.*;

public class PlaceTagReducer extends Reducer<Text,Text,Text,Text> {
    private Map<String,Integer> localityTable = new HashMap<String, Integer>();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {;
        int count = 0;
        String result = "";
        String year = "";
        String locality = "";
        List<String> placesList = new ArrayList<String>();

        for (Text currentValue:values){
            String[] tagDateArray = currentValue.toString().split("\t");
            result += tagDateArray[0]+" ";
            String pictureYear = tagDateArray[1].substring(0,4);
            String[] placesArray = tagDateArray[2].substring(1).split("/");
            int placesLength = placesArray.length >3? 3: placesArray.length;
            for (int i = 0; i<placesLength ; i++){
                String placeTmp = placesArray[i].replace("+","").toLowerCase().trim();
                if(!Utils.contains(placesList,placeTmp)){
                    placesList.add(placeTmp);
                }
            }
            if(year.indexOf(pictureYear) == -1)
                year += pictureYear+" ";
            count++;
        }
        for (String placeTmp: placesList){

            locality+= placeTmp+" ";
        }
        localityTable.put(key.toString()+"\t"+result.substring(0,result.length()-1)+"\t"+year.substring(0,year.length()-1)
                +"\t"+locality.substring(0,locality.length()-1),count);

    }
    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
        Map<String, Integer> sortedMap = sortByValues(localityTable);
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
            Map<String, Integer> sortedTags = PlaceTagReducer.sortByValues(tagTable);
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
    public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}
