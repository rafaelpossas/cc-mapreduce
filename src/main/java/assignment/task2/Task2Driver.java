package assignment.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out>");
            System.exit(2);
        }




        Job placeFilterJob = Job.getInstance(conf,"Task 2 - Place Mapper");
        placeFilterJob.setNumReduceTasks(0);
        placeFilterJob.setJarByClass(Task2Driver.class);
        placeFilterJob.setMapperClass(PlaceCountryMapper.class);
        placeFilterJob.setOutputKeyClass(Text.class);
        placeFilterJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(placeFilterJob, new Path("tmp/places"));
        placeFilterJob.waitForCompletion(true);


        Job joinJob = Job.getInstance(conf, "Task 2 - Country Reducer");
        joinJob.addCacheFile(new Path("tmp/places/part-m-00000").toUri());
        joinJob.setNumReduceTasks(10);
        joinJob.setJarByClass(Task2Driver.class);
        joinJob.setMapOutputKeyClass(Text.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(joinJob, new Path(otherArgs[1]),
                TextInputFormat.class, CountryPhotoMapper.class);
        joinJob.setCombinerClass(CountryPhotoCombiner.class);
        joinJob.setReducerClass(CountryLocalityReducer.class);
        TextOutputFormat.setOutputPath(joinJob, new Path(otherArgs[2]));
        joinJob.waitForCompletion(true);


        // remove the temporary path
        FileSystem.get(conf).delete(new Path("tmp"), true);

    }
}
