package utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.ListenerList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by rafaelpossas on 13/04/16.
 */
public class Utils {

    public static boolean contains(List<String> list, String string){
        for(String tmp: list){
            if(tmp.equals(string)){
                return true;
            }
        }
        return false;
    }

    public static String getLocality(String url){
        String[] localities = url.split("/");
        String locality;
        if(localities.length >=4){
            locality = localities[3];
        }else{
            locality = localities[2];
        }
        return locality;
    }
    public static String getNeighbourhood(String url){
        String[] localities = url.split("/");
        String neighborhoud = "";
        if(localities.length >4)
            neighborhoud = localities[4];
        return neighborhoud;
    }
    public static String countNeighbourhood(String neighbourhood){
        String[] neighbourhoodArray = neighbourhood.split(" ");
        List<String> countedNeighbourhoods = new ArrayList<String>();
        String result = "";
        String spacer = "";
        for (String currentNeighbourhood: neighbourhoodArray){
            if(!result.equals("") && spacer.equals(""))
                spacer = " ";
            if(!Utils.contains(countedNeighbourhoods,currentNeighbourhood) && !currentNeighbourhood.equals("")){
                result+= spacer+currentNeighbourhood+"<>"+ StringUtils.countMatches(neighbourhood,currentNeighbourhood);
                countedNeighbourhoods.add(currentNeighbourhood);
            }

        }
        return result;
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
    public static Map.Entry<String, Integer> getMaxEntry(Map<String, Integer> map) {
        Map.Entry<String, Integer> maxEntry = null;

        for (Map.Entry<String, Integer> entry : map.entrySet())
        {
            if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
            {
                maxEntry = entry;
            }
        }
        return maxEntry;
    }
    public static boolean getMergeInHdfs(String src, String dest,Configuration config) throws IllegalArgumentException, IOException {
        FileSystem fs = FileSystem.get(config);
        Path srcPath = new Path(src);
        Path dstPath = new Path(dest);

        // Check if the path already exists
        if (!(fs.exists(srcPath))) {
            return false;
        }

        if (!(fs.exists(dstPath))) {
            return false;
        }
        return FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, config, null);
    }
}
