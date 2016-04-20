package assignment.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by rafaelpossas on 4/20/16.
 */
public class Top50Mapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        if(dataArray.length == 3){
            String locality = dataArray[0];
            Integer totalPhotos = Integer.parseInt(dataArray[1]);
            String tags = dataArray[2];
            context.write(new Text(locality),new Text(totalPhotos+"\t"+tags));
        }

    }
}
