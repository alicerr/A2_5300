import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankBlockReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> vals, Context context){
		long pass = context.getCounter(PageRankEnum.PASS).getValue();
		if (pass == 0){
			String toList = "";
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
			context.getCounter(PageRankEnum.TOTAL_NODES).increment(1);
		} else {
			double redistributeValue = context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue()/(context.getCounter(PageRankEnum.TOTAL_NODES).getValue() * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
			double newPageRank = CONST.RANDOM_SURFER*CONST.BASE_PAGE_RANK + redistributeValue;
			double oldPageRank = 0.;
			String toList = "";
			for (Text val : vals){
				if (val.toString().contains("|")){
					String[] info = val.toString().split("|");
					toList = info[0];
					oldPageRank = Double.parseDouble(info[1]);
				} else {
					newPageRank += CONST.DAMPING_FACTOR * Double.parseDouble(val.toString());
				}
				
			}
			double residualValue = Math.abs(newPageRank - oldPageRank)/newPageRank;
			context.getCounter(PageRankEnum.RESIDUAL_SUM).increment((long)(residualValue * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
			try {
				context.write(key, new Text(toList + "|" + newPageRank + "|" + residualValue));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
