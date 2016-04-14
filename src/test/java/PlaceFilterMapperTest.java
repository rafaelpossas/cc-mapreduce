import labs.join.PlaceFilterMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by rafaelpossas on 9/04/16.
 */
public class PlaceFilterMapperTest {

    MapDriver<Object,Text,Text,Text> mapDriver;
    String input_1 = "vPdcHdKYA5lJ_7G2.w\t12578015\t35.66\t109.509\tShaanxi, CN, China\t8\t/China/CN/Shaanxi";
    String input_2 = "N2Z02BGcB5zqf0_Lpw\t56013309\t-8.262\t115.353\tBatur Utara, Bali, Indonesia\t7\t/Indonesia/Bali/Batur+Utara";
    String input_3 = "cbt2m.eeCZ5Fd_E\t782053\t46.509\t6.647\tChamblandes, Pully, VD, CH, Switzerland\t22\t/Switzerland/VD/Pully/Chamblandes";
    String input_4 = "biA1DmicBJUtZYCkOw\t55926442\t17.117\t53.988\tAl Nabi Ayoob, Zufar, Oman\t7\t/Oman/Zufar/Al+Nabi+Ayoob";
    String input_5 = "xPOpvDeYBJkdlw\t15596\t51.504\t-0.609\tChalvey, Slough, England, GB, United Kingdom\t22\t/United+Kingdom/England/Slough/Chalvey";

    @Before
    public void setUp() {
        PlaceFilterMapper mapper = new PlaceFilterMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.getConfiguration().set("mapper.placeFilter.country","China");
        mapDriver.withInput(new LongWritable(), new Text(input_1));
        mapDriver.withInput(new LongWritable(), new Text(input_2));
        mapDriver.withInput(new LongWritable(), new Text(input_3));
        mapDriver.withInput(new LongWritable(), new Text(input_4));
        mapDriver.withInput(new LongWritable(), new Text(input_5));

        mapDriver.withOutput(new Text("vPdcHdKYA5lJ_7G2.w"),new Text("/China/CN/Shaanxi"));

        mapDriver.runTest();
    }
}
