package assignment.task1;

import labs.reducesidejoin.JoinGroupComparator;
import labs.reducesidejoin.JoinPartitioner;
import labs.reducesidejoin.TextIntPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.Utils;

public class Task1Driver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setLong("mapreduce.task.timeout", 0);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out>");
            System.exit(2);
        }



        Job placeFilterJob = Job.getInstance(conf, "Task 1 Place Mapper");
        placeFilterJob.setNumReduceTasks(0);
        placeFilterJob.setJarByClass(Task1Driver.class);
        placeFilterJob.setMapperClass(PlaceTypeMapper.class);
        placeFilterJob.setOutputKeyClass(Text.class);
        placeFilterJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(placeFilterJob, new Path("tmp/places"));
        placeFilterJob.waitForCompletion(true);


        Job joinJob = Job.getInstance(conf, "Task1 - Place/Tag Map-Reduce");
        joinJob.addCacheFile(new Path("tmp/places/part-m-00000").toUri());
        joinJob.setJarByClass(Task1Driver.class);
        joinJob.setNumReduceTasks(10);
        joinJob.setCombinerClass(PlaceTagCombiner.class);
        joinJob.setReducerClass(PlaceTagReducer.class);
        joinJob.setMapOutputKeyClass(Text.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(joinJob, new Path(otherArgs[1]), TextInputFormat.class, PlaceTagMapper.class);
        TextOutputFormat.setOutputPath(joinJob, new Path("tmp/localities"));
        joinJob.waitForCompletion(true);
        // remove the temporary path

        Job top50Job = Job.getInstance(conf,"Task1 - Top 50 Reducer");
        top50Job.setJarByClass(Task1Driver.class);
        top50Job.setReducerClass(Top50Reducer.class);
        top50Job.setNumReduceTasks(1);
        top50Job.setMapOutputKeyClass(Text.class);
        top50Job.setMapOutputValueClass(Text.class);
        top50Job.setOutputKeyClass(Text.class);
        top50Job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(top50Job, new Path("tmp/localities"), TextInputFormat.class, Top50Mapper.class);
        TextOutputFormat.setOutputPath(top50Job, new Path(otherArgs[2]));
        top50Job.waitForCompletion(true);

        FileSystem.get(conf).delete(new Path("tmp"), true);

    }
}
