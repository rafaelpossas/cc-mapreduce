import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import labs.usertag.TagCombiner;
import labs.usertag.TagMapper;
import labs.usertag.TagReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserTagMapReduceTest {
    MapDriver<LongWritable,Text,Text,Text> mapDriver;
    ReduceDriver<Text,Text,Text,Text> reduceDriver;
    ReduceDriver<Text,Text,Text,Text> combinerDriver;
    MapReduceDriver<LongWritable,Text,Text,Text,Text,Text> mapReduceDriver;
    String input_1 = "2048252769\t48889082718@N01\tdog francis lab labrador nap sleepy tired labradorretriever iphone\t2007-11-19 17:49:49\tRRBihiubApl0OjTtWA\t16";
    String input_2 = "1684446990\t48889082718@N01\tsanfrancisco night twinpeaks jof jonathanlassoff thejof\t2007-10-17 22:18:34\tz.YyGqSbApmlndcfqg\t14";
    String input_3 = "1667195242\t48889082718@N01\tsanfrancisco northbeach\t2007-10-20 16:42:35\t9xdhxY.bAptvBjHo\t15";
    String input_4 = "1667195226\t48889082718@N02\tsanfrancisco northbeach\t2007-10-20 16:44:56\tWhLbjlWbAplfF0e0cg\t15";
    String input_5 = "1667195170\t48889082718@N03\tsanfrancisco northbeach\t2007-10-20 16:42:43\t9xdhxY.bAptvBjHo\t15";

    @Before
    public void setUp() {
        TagMapper mapper = new TagMapper();
        TagReducer reducer = new TagReducer();
        TagCombiner combiner = new TagCombiner();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        combinerDriver = ReduceDriver.newReduceDriver(combiner);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
        mapReduceDriver.withCombiner(combiner);

    }

    @Test
    public void testMapper() throws IOException{

        mapDriver.withInput(new LongWritable(), new Text(input_1));
        mapDriver.withInput(new LongWritable(), new Text(input_2));
        mapDriver.withInput(new LongWritable(), new Text(input_3));
        mapDriver.withInput(new LongWritable(), new Text(input_4));
        mapDriver.withInput(new LongWritable(), new Text(input_5));

        mapDriver.withOutput(new Text("dog"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("francis"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("lab"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("labrador"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("nap"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("sleepy"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("tired"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("labradorretriever"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("iphone"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("night"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("twinpeaks"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("jof"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("jonathanlassoff"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("thejof"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("northbeach"),new Text("48889082718@N01"));
        mapDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N02"));
        mapDriver.withOutput(new Text("northbeach"),new Text("48889082718@N02"));
        mapDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N03"));
        mapDriver.withOutput(new Text("northbeach"),new Text("48889082718@N03"));

        mapDriver.runTest();
    }
    @Test
    public void testCombiner() throws IOException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("48889082718@N01"));
        values.add(new Text("48889082718@N01"));
        values.add(new Text("48889082718@N02"));
        values.add(new Text("48889082718@N03"));
        combinerDriver.withInput(new Text("sanfrancisco"),values);
        combinerDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N03=1"));
        combinerDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N01=2"));
        combinerDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N02=1"));
        combinerDriver.runTest();
    }
    @Test
    public void testReducer() throws IOException{
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("7556490@N05=2"));
        reduceDriver.withInput(new Text("protest"),values);
        reduceDriver.withOutput(new Text("protest"),new Text("7556490@N05=2"));
        reduceDriver.runTest();
    }
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(input_2));
        mapReduceDriver.withInput(new LongWritable(), new Text(input_3));
        mapReduceDriver.withInput(new LongWritable(), new Text(input_4));
        mapReduceDriver.withInput(new LongWritable(), new Text(input_5));
        mapReduceDriver.withOutput(new Text("jof"),new Text("48889082718@N01=1"));
        mapReduceDriver.withOutput(new Text("jonathanlassoff"),new Text("48889082718@N01=1"));
        mapReduceDriver.withOutput(new Text("night"),new Text("48889082718@N01=1"));
        mapReduceDriver.withOutput(new Text("northbeach"),new Text("48889082718@N03=1,48889082718@N01=1,48889082718@N02=1"));
        mapReduceDriver.withOutput(new Text("sanfrancisco"),new Text("48889082718@N03=1,48889082718@N01=2,48889082718@N02=1"));
        mapReduceDriver.withOutput(new Text("thejof"),new Text("48889082718@N01=1"));
        mapReduceDriver.withOutput(new Text("twinpeaks"),new Text("48889082718@N01=1"));

        mapReduceDriver.runTest();

    }

}
