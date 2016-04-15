package assignment.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
 * Reads the place.txt file and creates a intermediate file for join
 *
 *  Input
 *  place-id  woeid  latitude  longitude  place-name  place-type-id  place-url
 *
 *  Output
 *  place-id  place-type-id country place-url
 *
 *
 */
public class PlaceCountryMapper extends Mapper<Object, Text, Text, Text>{
    private Text placeId= new Text(), placeName = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 7){ // a not complete record with all data
            return; // don't emit anything
        }
        String[] location = dataArray[6].substring(1).split("/");
        String country = location[0].replace("+","").toLowerCase().trim();
        placeId.set(dataArray[0]);
        placeName.set(dataArray[5]+"\t"+country+"\t"+dataArray[6]);
        context.write(placeId, placeName);


    }
}
