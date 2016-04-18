package assignment.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rafaelpossas on 4/18/16.
 */
public class PlaceTagCombiner extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalPhotos = 0;
        String tags = "";
        String years = "";
        String places = "";
        List<String> placesList = new ArrayList<String>();

        for (Text currentValue:values){
            String[] tagDateArray = currentValue.toString().split("\t");
            tags += tagDateArray[0]+" ";
            String pictureYear = tagDateArray[1].substring(0,4);
            String[] placesArray = tagDateArray[2].substring(1).split("/");
            int placesLength = placesArray.length >3? 3: placesArray.length;
            for (int i = 0; i<placesLength ; i++){
                String placeTmp = placesArray[i].replace("+","").toLowerCase().trim();
                if(!Utils.contains(placesList,placeTmp)){
                    placesList.add(placeTmp);
                }
            }
            if(years.indexOf(pictureYear) == -1)
                years += pictureYear+" ";
            totalPhotos++;
        }
        for (String placeTmp: placesList){

            places+= placeTmp+" ";
        }
        context.write(key,new Text(tags+"\t"+years
                +"\t"+places+"\t"+totalPhotos));


    }
}
