package labs.reducesidejoin;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
/**
 * Partition basaed on Key, ignore the int
 * @author Ying Zhou
 *
 */
public class JoinPartitioner extends Partitioner<TextIntPair,Text> {

	@Override
	public int getPartition(TextIntPair key, Text value, int numPartition) {
		// TODO Auto-generated method stub
		return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartition;
	}
		
}
