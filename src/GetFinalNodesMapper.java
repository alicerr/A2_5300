import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * Implements the last Job map functionality for All main functions
 * @author Alice, Spencer, Garth
 *
 */
public class GetFinalNodesMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	
/** Overrides map
 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
 */
public void map(LongWritable keyin, Text val, Context context){
		
		// Separate Data and fill nodes HashMap
		String[] info = val.toString().split(CONST.L0_DIV, -1);
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		Util.fillMapsFromBlockString(info, nodes, null, null);

		// Get data and write out for first two nodes in each block
		for (Node n : nodes.values()){
			if (Util.isInFirstTwoIndex((int) keyin.get(), n.id)){
				try {
					context.write(new LongWritable(n.id), new Text(n.getPR()+ ""));
				} catch (IOException | InterruptedException e) {
					
					e.printStackTrace();
				}
			}
		}
	
	}
}

