package block;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.CONST;
import common.Edge;
import common.Node;
import common.PageRankEnum;
import common.Util;


/**
 * Implements the subsequent Jobs (after first pass) map functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankBlockMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	
/** Overrides map
 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
 */
public void map(LongWritable keyin, Text val, Context context){
		
		// keep block text, we don't need to recreate these
		// Create the hashmaps we will be using
		String[] info = val.toString().split(CONST.L0_DIV, -1);
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		HashMap<Integer, ArrayList<Edge>> outerEdges = new HashMap<Integer, ArrayList<Edge>>();
		double sinks = Util.fillMapsFromBlockString(info, nodes, null, outerEdges);
		

		// Handle Sinks
		context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment((long) (sinks * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
		
		// For each outer edge in all lists
		for (ArrayList<Edge> ae : outerEdges.values()){
			for (Edge e : ae){
				try { // Get all the PR values from edges of block
					// (toBlockID -> {toNode, PRonEdge})
					context.write(new LongWritable(Util.idToBlock(e.to)), new Text(CONST.INCOMING_EDGE_MARKER + CONST.L0_DIV + e.to + CONST.L0_DIV + nodes.get(e.from).prOnEdge()));
				} catch (IOException | InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		try { // Write keyin and val for next round
			// (blockID -> {Block data})
			context.write(keyin, val);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
}
	

