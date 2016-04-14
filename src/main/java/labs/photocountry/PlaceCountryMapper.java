package labs.photocountry;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This mapper is used to filter place names containing a particular country name
 *
 * input format:
 * place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
 *
 * output format:
 * place_id \t place_url
 *
 * The country name is stored as a property in the job's configuration object.
 *
 * The configuration object can be obtained from the mapper/reducer's context object
 *
 * Because all calls to the map function needs to use the country name value, we save it
 * as an instance variable countryName and set the value of it in the setup method.
 * The setup method is called after the mapper is created. It is before any call of the
 * first map method.
 *
 * @author Ying Zhou
 *
 */
public class PlaceCountryMapper extends Mapper<Object, Text, Text, Text> {
    private String[] countries;
    private Text placeId= new Text(), placeUrl = new Text();

    public void setup(Context context){
        String tmpCountries = "";
        tmpCountries = context.getConfiguration().get("mapper.placeFilter.countries", tmpCountries);
        countries = tmpCountries.split(";");
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 7){ // a not complete record with all data
            return; // don't emit anything
        }
        String place = dataArray[6].split("/")[1];
        if (ArrayUtils.contains(countries,place)){
            placeId.set(dataArray[0]);
            placeUrl.set(place);
            context.write(placeId, placeUrl);
        }

    }

}
