import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PageRankBlockMapper extends
		Mapper<IntWritable, Text, IntWritable, Text> {
	
	public void mapper(IntWritable keyin, Text val, Context context){
		
		//keep block text, we don't need to recreate these
		String[] info = val.toString().split(CONST.L0_DIV);
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		HashMap<Integer, ArrayList<Edge>> outerEdges = new HashMap<Integer, ArrayList<Edge>>();

		double sinks = Util.fillMapsFromBlockString(info, nodes, null, outerEdges);
		

		context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment((long) (sinks * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
		for (ArrayList<Edge> ae : outerEdges.values()){
			for (Edge e : ae){
				try {
					context.write(new IntWritable(Util.idToBlock(e.to)), new Text(CONST.INCOMING_EDGE_MARKER + CONST.L0_DIV + e.to + CONST.L0_DIV + nodes.get(e.from).prOnEdge()));
				} catch (IOException | InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		try {
			context.write(keyin, val);
		} catch (IOException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	
	}
}
	

