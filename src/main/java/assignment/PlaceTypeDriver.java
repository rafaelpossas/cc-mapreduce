package assignment;

import assignment.task1.PlaceTypeMapper;
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
public class PlaceTypeDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ReplicationJoinDriver <inPlace> <inPhoto> <out> ");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Replication Join");
        job.setJarByClass(PlaceTypeDriver.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(PlaceTypeMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
