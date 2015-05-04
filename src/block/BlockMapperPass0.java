package block;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.CONST;
import common.Util;


/**
 * Implements the first Job map functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class BlockMapperPass0 extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	
	/** Overrides the map function
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable keyin, Text val, Context context){
		
		// Grabs a slightly pre-processed version of edges.txt line
		String[] info = val.toString().split(" ");
		try {
			// Separates into a From and To defining the edge and fromBlock toBlock
			double select = Double.parseDouble(info[0]);
			int fromInt = Integer.parseInt(info[1]);
			int toInt = Integer.parseInt(info[2]);
			int fromBlock = Util.idToBlock(fromInt);
			int toBlock = Util.idToBlock(toInt);
			
			if (Util.retainEdgeByNodeID(select)){ // If we should keep the edge (based on netid) 
				try {
					context.write(new LongWritable(fromBlock), new Text(CONST.SEEN_EDGE_MARKER + CONST.L0_DIV + fromInt + CONST.L0_DIV + toInt));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else { // Else we shouldn't keep the edge and we save it with no toInt
				try {
					context.write(new LongWritable(fromBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + fromInt));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			try { // Write toBlock maps to with just a toInt. In case we don't see this node again (we don't want to lose a node)
				context.write(new LongWritable(toBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + toInt));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
}