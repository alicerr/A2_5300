import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Implements the subsequent Jobs reduce functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankBlockReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {


		/** Overrites reduce
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(LongWritable key, Iterable<Text> vals, Context context){

			// Sets up hashmaps for vals
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			HashMap<Integer, ArrayList<Edge>> innerEdges = new HashMap<Integer, ArrayList<Edge>>();
			HashMap<Integer, Double> outerEdges = new HashMap<Integer, Double>();
			double inBlockSink = 0.;
			String outerEdgesString = "";
			String innerEdgesString = "";
			
			// Get values passed into function
			for (Text val : vals){
				String[] info = val.toString().split(CONST.L0_DIV, -1);
				byte marker = Byte.parseByte(info[CONST.MARKER_INDEX_L0]);
				
				// Block Data
				if (marker == CONST.ENTIRE_BLOCK_DATA_MARKER){
					inBlockSink = Util.fillMapsFromBlockString(info, nodes, innerEdges, null);
					outerEdgesString = info[CONST.OUTER_EDGE_LIST];
					innerEdgesString = info[CONST.INNER_EDGE_LIST];
					
				} 
				// Incoming Edged Data
				else if (marker == CONST.INCOMING_EDGE_MARKER){
					OuterEdgeValue incoming = new OuterEdgeValue(info);
					if (outerEdges.containsKey(incoming.to)){ // Check if we already know about this edge and add PR
						outerEdges.put(incoming.to, outerEdges.get(incoming.to) + incoming.pr);
					} else {
						outerEdges.put(incoming.to, incoming.pr);
					}
				}
			}
			
			// Get Data from counters for calculations
			double outOfBlockSink = CONST.DAMPING_FACTOR*((context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue() + .5)/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG - inBlockSink);
			double totalNodes = context.getConfiguration().getLong("TOTAL_NODES", 685230);
			double basePageAddition = CONST.RANDOM_SURFER * CONST.BASE_PAGE_RANK + outOfBlockSink/totalNodes;
			org.apache.hadoop.mapreduce.Counter innerBlockRounds = context.getCounter(PageRankEnum.INNER_BLOCK_ROUNDS);
			// Set up Maps for each pass of loop below
			HashMap<Integer, Node> nodesLastPass = new HashMap<Integer, Node>();
			HashMap<Integer, Node> nodesThisPass = new HashMap<Integer, Node>();
			boolean converged = false;
			double residualSum = 0.;
			double newInBlockSink = 0.;
			
			// Each node is put into nodesLastPass for first pass
			for (Node n : nodes.values()){
				nodesLastPass.put(n.id, new Node(n));
			}
			
			// Run until converged in block
			while (!converged){
				double baseInBlockPageAddition = CONST.DAMPING_FACTOR * (inBlockSink/(double)totalNodes);
				// For each node from last pass
				for (Node n : nodesLastPass.values()){
					
					// Base PR
					double pr = basePageAddition + baseInBlockPageAddition;
					
					// Incoming PR added in
					if (outerEdges.containsKey(n.id))
						pr += CONST.DAMPING_FACTOR * outerEdges.get(n.id);
					
					// In Block PR added int
					if (innerEdges.containsKey(n.id))
						for (Edge e : innerEdges.get(n.id))
							pr += CONST.DAMPING_FACTOR * nodesLastPass.get(e.from).prOnEdge();
					
					// Calculate Residual
					double residual = Math.abs((pr - n.getPR()))/pr;
					residualSum += residual;
					
					// Add node to a nodesThisPass since we have processed it
					Node nPrime = new Node(n);
					nPrime.setPR(pr);
					nodesThisPass.put(nPrime.id, nPrime);
					
					// Check for sink
					if (nPrime.edges() == 0)
						newInBlockSink += pr;
					
					
					
				}
				// Reset Holders for next round.
				// nodesThisPass becomes lastPass
				nodesLastPass = nodesThisPass;
				// Reset NodesThisPass
				nodesThisPass = new HashMap<Integer, Node>();
				// Pass sink on
				inBlockSink = newInBlockSink;
				newInBlockSink = 0;
				
				// Check if we have converged
				converged = residualSum < CONST.RESIDUAL_SUM_DELTA;
				//System.out.println(key + " " + residualSum);
				residualSum = 0;
				innerBlockRounds.increment(1);
				
			}
			
			// Once we converge Calculate block data
			double residualSumOuter = 0.;
			for (Node n : nodesLastPass.values()){
				double residual = Math.abs((n.getPR() - nodes.get(n.id).getPR()))/n.getPR();
				residualSumOuter += residual;
			}
			context.getCounter(PageRankEnum.RESIDUAL_SUM).increment((long) (residualSumOuter * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
			
			// Save updated Block data
			String block = Util.getBlockDataAsString(nodesLastPass, innerEdgesString, outerEdgesString);
			try {
				context.write(key, new Text(block));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	

}
