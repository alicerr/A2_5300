import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PageRankBlockMapper extends
		Mapper<IntWritable, Text, IntWritable, Text> {
	
	public void mapper(IntWritable keyin, Text val, Context context){
		long pass = context.getCounter(PageRankEnum.PASS).getValue();
		if (pass == 0){
			String[] info = val.toString().split(" ");
			try {
				double select = Double.parseDouble(info[0]);
				int fromInt = Integer.parseInt(info[1]);
				int toInt = Integer.parseInt(info[2]);
				int fromBlock = Util.idToBlock(fromInt);
				int toBlock = Util.idToBlock(toInt);
				
				
				if (Util.retainEdgeByNodeID(select)){
					context.write(new IntWritable(fromBlock), new Text(CONST.SEEN_EDGE_MARKER + CONST.L0_DIV + fromInt + CONST.L0_DIV + toInt));
				} else {
					context.write(new IntWritable(fromBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + fromInt));
					
				}
				context.write(new IntWritable(toBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + fromInt));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} else {
			//keep block text, we don't need to recreate these
			String[] info = val.toString().split(CONST.L0_DIV);
			String[] nodesString = info[CONST.NODE_LIST].split(CONST.L1_DIV);
			
			String[] outerEdgesString = info[CONST.OUTER_EDGE_LIST].split(CONST.L1_DIV);
			String[] innerEdgesString = info[CONST.OUTER_EDGE_LIST].split(CONST.L1_DIV);
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			//HashMap<Integer, Edge> innerEdges = new HashMap<Integer, Edge>();
			HashMap<Integer, Edge> outerEdges = new HashMap<Integer, Edge>();
			for (String nodeString : nodesString){
				Node node = new Node(nodeString);
				nodes.put(node.id, node);
			}
			for (String edgeString : innerEdgesString){
				Edge e = new Edge(edgeString, true);
				nodes.get(e.from).addBranch();
			}
			for (String edgeString : outerEdgesString){
				Edge e = new Edge(edgeString, false);
				nodes.get(e.from).addBranch();
				outerEdges.put(e.from, e);
			}
			double sinks = 0.;
			for (Node n : nodes.values()){
				if (n.edges() == 0)
					sinks += n.getPR();
			}
			//TODO update sink value
			for (Edge e : outerEdges.values()){
				context.write(new IntWritable(Util.idToBlock(e.to)), new Text(CONST.INCOMING_EDGE_MARKER + CONST.L0_DIV + e.to + CONST.L0_DIV + nodes.get(e.from).prOnEdge()));
			}
		
		}
	}
}
