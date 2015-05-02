import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockReducerPass0 extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		
		StringBuffer nodes = new StringBuffer();
		StringBuffer innerEdges = new StringBuffer();
		StringBuffer outerEdges = new StringBuffer();
		HashSet<Integer> seenNodes = new HashSet<Integer>();
		for (Text val : vals){
			
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
				if (Util.idToBlock(to) == key.get()){
					innerEdges.append(CONST.L1_DIV + from + CONST.L2_DIV + to);
					
				} else {
					outerEdges.append(CONST.L1_DIV + from + CONST.L2_DIV + to);
				}
				
			}
			context.getCounter(PageRankEnum.TOTAL_NODES).increment(1);
		}
		if (nodes.length() == 0)
			nodes.append(CONST.L1_DIV);
		if (innerEdges.length() == 0)
			innerEdges.append(CONST.L1_DIV);
		if (outerEdges.length() == 0)
			outerEdges.append(CONST.L1_DIV);
		try {
			context.write(key, new Text(Util.getBlockDataAsString(nodes, innerEdges, outerEdges)));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		 
	}


}
