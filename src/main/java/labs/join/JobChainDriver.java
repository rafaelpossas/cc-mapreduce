package labs.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

/**
 * This is a sample program to chain the place filter job and replicated join job.
 *
 * @author Ying Zhou
 *
 */

public class JobChainDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out> [countryName]");
            System.exit(2);
        }

        // pass a parameter to mapper class
        if (otherArgs.length == 4){
            conf.set("mapper.placeFilter.country", otherArgs[3]);
        }

        Path tmpFilterOut = new Path("tmpFilterOut"); // a temporary output path for the first job

        Job placeFilterJob = Job.getInstance(conf, "Place Filter");
        placeFilterJob.setJarByClass(PlaceFilterDriver.class);
        placeFilterJob.setNumReduceTasks(0);
        placeFilterJob.setMapperClass(PlaceFilterMapper.class);
        placeFilterJob.setOutputKeyClass(Text.class);
        placeFilterJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
        placeFilterJob.waitForCompletion(true);

        Job joinJob = Job.getInstance(conf, "Replication Join");
        joinJob.addCacheFile(new Path("tmpFilterOut/part-m-00000").toUri());
        joinJob.setJarByClass(ReplicateJoinDriver.class);
        joinJob.setNumReduceTasks(0);
        joinJob.setMapperClass(ReplicateJoinMapper.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(joinJob, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(joinJob, new Path(otherArgs[2]));
        joinJob.waitForCompletion(true);
        // remove the temporary path
        FileSystem.get(conf).delete(tmpFilterOut, true);

    }
}