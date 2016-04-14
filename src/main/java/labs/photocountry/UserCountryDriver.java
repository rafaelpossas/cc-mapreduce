package labs.photocountry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by rafaelpossas on 10/04/16.
 */
public class UserCountryDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out> [countryNames]");
            System.exit(2);
        }

        // pass a parameter to mapper class
        if (otherArgs.length >= 4){
            String countries = "";
            for (int i =3; i< otherArgs.length ; i++){
                countries += otherArgs[i] + ";";
            }
            countries = StringUtils.removeEnd(countries,";");
            conf.set("mapper.placeFilter.countries", countries);
        }

        Path tmpFilterOut = new Path("tmpfilter"); // a temporary output path for the first job

        Job placeFilterJob = Job.getInstance(conf, "Place Country Filter");
        placeFilterJob.setJarByClass(PlaceCountryMapperDriver.class);
        placeFilterJob.setNumReduceTasks(0);
        placeFilterJob.setMapperClass(PlaceCountryMapper.class);
        placeFilterJob.setOutputKeyClass(Text.class);
        placeFilterJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
        placeFilterJob.waitForCompletion(true);


        Job joinJob = Job.getInstance(conf, "Replication Join");
        joinJob.addCacheFile(new Path("tmpfilter/part-m-00000").toUri());
        joinJob.setJarByClass(UserCountryMapperDriver.class);
        joinJob.setNumReduceTasks(1);
        joinJob.setMapperClass(UserCountryMapper.class);
        joinJob.setReducerClass(UserCountryReducer.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(joinJob, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(joinJob, new Path(otherArgs[2]));
        joinJob.waitForCompletion(true);
        // remove the temporary path


    }
}
