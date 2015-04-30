import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockReducerPass0 extends
		Reducer<LongWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> vals, Context context){
	
		StringBuffer nodes = new StringBuffer();
		StringBuffer innerEdges = new StringBuffer();
		StringBuffer outerEdges = new StringBuffer();
		for (Text val : vals){
			String[] info = val.toString().split(CONST.L0_DIV);
			byte marker = Byte.parseByte(info[0]);
			if (marker == CONST.SEEN_NODE_MARKER){
				nodes.append(CONST.L1_DIV + info[1] + CONST.L2_DIV + CONST.BASE_PAGE_RANK);
			} else if (marker == CONST.SEEN_EDGE_MARKER){
				int from = Integer.parseInt(info[1]);
				int to = Integer.parseInt(info[2]);
				if (Util.idToBlock(to) == key.get()){
					innerEdges.append(CONST.L1_DIV + from + CONST.L2_DIV + to);
				} else {
					outerEdges.append(CONST.L1_DIV + from + CONST.L2_DIV + to);
				}
				
			}
			context.getCounter(PageRankEnum.TOTAL_NODES).increment(1);
		}
		Util.getBlockDataAsString(nodes, innerEdges, outerEdges);
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
