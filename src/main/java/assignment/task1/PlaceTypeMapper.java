package assignment.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by rafaelpossas on 10/04/16.
 */
public class PlaceTypeMapper extends Mapper<Object, Text, Text, Text> {
    private Text placeId= new Text(), placeName = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 7){ // a not complete record with all data
            return; // don't emit anything
        }
        String placeType = dataArray[5];
        if (placeType.equals("7") || placeType.equals("22")){
            String[] localities = dataArray[6].split("/");
            String locality;
            if(localities.length >=4){
                locality = localities[3].replace("+","").toLowerCase();
            }else{
                locality = localities[2].replace("+","").toLowerCase();
            }
            placeId.set(dataArray[0]);
            placeName.set(locality+"\t"+dataArray[6]);
            context.write(placeId, placeName);
        }

    }
}
