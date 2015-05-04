package block;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import common.CONST;
import common.PageRankEnum;
import common.Util;


/**
 * Implements the first Job reduce functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class BlockReducerPass0 extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	/** Overrides the Reduce function
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		
		StringBuffer nodes = new StringBuffer();
		StringBuffer innerEdges = new StringBuffer();
		StringBuffer outerEdges = new StringBuffer();
		HashSet<Integer> seenNodes = new HashSet<Integer>();
		// Loops through all the values associated with the key
		for (Text val : vals){
			
			// Each value is checked if we have seen it, seen the edge, and add the node
			// to seenNodes and nodes appropriately
			String[] info = val.toString().split(CONST.L0_DIV, -1);
			byte marker = Byte.parseByte(info[0]);
			if (marker == CONST.SEEN_NODE_MARKER){
				int nodeID = Integer.parseInt(info[1]);
				if (!seenNodes.contains(nodeID)){
					nodes.append(CONST.L1_DIV + info[1] + CONST.L2_DIV + CONST.BASE_PAGE_RANK);
					seenNodes.add(nodeID);
				}
			} else if (marker == CONST.SEEN_EDGE_MARKER){
				int from = Integer.parseInt(info[1]);
				int to = Integer.parseInt(info[2]);
				if (!seenNodes.contains(from)){
					nodes.append(CONST.L1_DIV + from + CONST.L2_DIV + CONST.BASE_PAGE_RANK);
					seenNodes.add(from);
				}
				// Specifies if we are currently looking at an internal edge or external edge for a block
				if (Util.idToBlock(to) == key.get()){
					innerEdges.append(CONST.L1_DIV + from + CONST.L2_DIV + to);
					
				} else {
					outerEdges.append(CONST.L1_DIV + from + CONST.L2_DIV + to);
				}
				
			}
			// Keep track of how many nodes we see... more of a sanity check since we know how many there are.
			context.getCounter(PageRankEnum.TOTAL_NODES).increment(1);
		}
		// Add dividers as necessary for empty strings
		if (nodes.length() == 0)
			nodes.append(CONST.L1_DIV);
		if (innerEdges.length() == 0)
			innerEdges.append(CONST.L1_DIV);
		if (outerEdges.length() == 0)
			outerEdges.append(CONST.L1_DIV);
		try {
			// Write out block data for next job
			context.write(key, new Text(Util.getBlockDataAsString(nodes, innerEdges, outerEdges)));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		 
	}


}
