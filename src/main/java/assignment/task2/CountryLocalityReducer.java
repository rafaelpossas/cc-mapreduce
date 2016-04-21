package assignment.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by rafaelpossas on 4/15/16.
 */
public class CountryLocalityReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, Map<String, String>> countryLocalityUrl = new HashMap<String, Map<String, String>>();
    private Map<String, Map<String, Integer>> countryLocalityCount = new HashMap<String, Map<String, Integer>>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, String> localityURL = null;
        Map<String, Integer> localityCount = null;
        try {
            for (Text value : values) {
                String url = value.toString().split(";")[1];
                String currentLocality = Utils.getLocality(url);
                String currentNeighbourhood = Utils.getNeighbourhood(url);
                if (localityURL == null) {
                    localityURL = new HashMap<String, String>();
                    localityCount = new HashMap<String, Integer>();
                    localityURL.put(currentLocality, currentNeighbourhood);
                    localityCount.put(currentLocality, 1);
                    countryLocalityUrl.put(key.toString(), localityURL);
                    countryLocalityCount.put(key.toString(), localityCount);

                } else {
                    if (localityURL.get(currentLocality) == null) {
                        localityURL.put(currentLocality.toString(), currentNeighbourhood);
                        localityCount.put(currentLocality.toString(), 1);

                    } else {
                        String neighbourhoods = localityURL.get(currentLocality);
                        Integer totalPhotos = localityCount.get(currentLocality);
                        localityURL.put(currentLocality, neighbourhoods + " " + currentNeighbourhood);
                        localityCount.put(currentLocality, ++totalPhotos);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String country : countryLocalityCount.keySet()) {
            Map<String, Integer> orderedTapLocalityCount = Utils.sortByValues(countryLocalityCount.get(country));
            countryLocalityCount.put(country, orderedTapLocalityCount);
        }


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        countryLocalityCount = new TreeMap<String, Map<String, Integer>>(countryLocalityCount);
        for (String key : countryLocalityCount.keySet()) {
            int top10 = 0;
            Map<String, String> localityURL = countryLocalityUrl.get(key.toString());
            Map<String, Integer> localityCount = countryLocalityCount.get(key.toString());
            Map<String, Integer> neighbourhoodCountMap;
            String result = "";
            for (String localKey : localityCount.keySet()) {
                if (top10 < 10) {
                    int totalPhotos = localityCount.get(localKey);
                    String[] neighbourhoodCount = Utils.countNeighbourhood(localityURL.get(localKey)).split(" ");
                    neighbourhoodCountMap = new HashMap<String, Integer>();
                    for (String count : neighbourhoodCount) {
                        if (!count.equals("")) {
                            try{
                                neighbourhoodCountMap.put(count.split("<>")[0], Integer.parseInt(count.split("<>")[1]));
                            }catch (Exception e){
                                e.printStackTrace();
                            }

                        }
                    }
                    Map.Entry<String, Integer> topNeighbourhood = Utils.getMaxEntry(neighbourhoodCountMap);
                    if (topNeighbourhood != null) {
                        result += "{" + localKey + ":" + totalPhotos + " " + topNeighbourhood.getKey() + ": " + topNeighbourhood.getValue() + "} ";
                    } else {
                        result += "{" + localKey + ":" + totalPhotos + "} ";
                    }
                }
                top10++;
            }
            context.write(new Text(key), new Text(result));

        }

    }
}
