import labs.join.ReplicateJoinMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by rafaelpossas on 9/04/16.
 */
public class ReplicateJoinTest {

    MapDriver<Object,Text,Text,Text> mapDriver;
    String place_1 = "jq2wyd.bAJj4GmDx\t/China/Gansu/Anning";
    String place_2 = "0J3au1ebB52EckSYEA\t/China/Guangxi/Beihai";
    String place_3 = "KPI4T4ebAJlISM4_\t/China/Chongqing/Fengjie";

    String input_1 = "2048252769\t48889082718@N01\tdog francis lab labrador nap sleepy tired labradorretriever iphone\t2007-11-19 17:49:49\tjq2wyd.bAJj4GmDx\t16";
    String input_2 = "1684446990\t48889082718@N01\tsanfrancisco night twinpeaks jof jonathanlassoff thejof\t2007-10-17 22:18:34\t0J3au1ebB52EckSYEA\t14";
    String input_3 = "1667195242\t48889082718@N01\tsanfrancisco northbeach\t2007-10-20 16:42:35\tKPI4T4ebAJlISM4_\t15";

    @Before
    public void setUp() {
        ReplicateJoinMapper mapper = new ReplicateJoinMapper();

        mapper.putPlaceTable(place_1);
        mapper.putPlaceTable(place_2);
        mapper.putPlaceTable(place_3);

        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input_1));

        mapDriver.withOutput(new Text("2048252769"),new Text("2007-11-19 17:49:49"+ "\t"+"/China/Gansu/Anning"));
        mapDriver.runTest();
    }

}
