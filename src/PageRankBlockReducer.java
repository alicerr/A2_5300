import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * Implements the subsequent Jobs reduce functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankBlockReducer extends
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {


		/** Overrites reduce
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(LongWritable key, Iterable<BytesWritable> vals, Context context){

			// Sets up hashmaps for vals
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			HashMap<Integer, ArrayList<Edge>> innerEdges = new HashMap<Integer, ArrayList<Edge>>();
			HashMap<Integer, ArrayList<Edge>> o = new HashMap<Integer, ArrayList<Edge>>();
			HashMap<Integer, Double> outerEdges = new HashMap<Integer, Double>();
			double inBlockSink = 0.;
			String outerEdgesString = "";
			String innerEdgesString = "";
			int counter = 0;
			double totalIncoming = 0.;
			// Get values passed into function
			for (BytesWritable val : vals){
				String[] info = val.toString().split(CONST.L0_DIV, -1);
				byte marker = val.getBytes()[0];
				
				// Block Data
				if (marker == CONST.ENTIRE_BLOCK_DATA_MARKER){
					inBlockSink = Util.fillBlockFromByteBuffer(ByteBuffer.wrap(val.getBytes()), nodes, innerEdges, o);
					
					
				} 
				// Incoming Edged Data
				else if (marker == CONST.INCOMING_EDGE_MARKER){
					ByteBuffer b = ByteBuffer.wrap(val.getBytes());
					b.get();
					int to = b.getInt();
					double pr = b.getDouble();
					//System.out.println(to + "->" + pr);
					totalIncoming += pr;
					if (outerEdges.containsKey(to)){ // Check if we already know about this edge and add PR
						outerEdges.put(to, outerEdges.get(to) + pr);
						
					} else {
						outerEdges.put(to, pr);
					}
					counter++;
				}
			}

			//System.out.println("got " + counter + " edges");
			//System.out.println(outerEdges);
			// Get Data from counters for calculations
			double sinkPerNode = context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue()/CONST.SIG_FIG_FOR_TINY_DOUBLE_TO_LONG/CONST.TOTAL_NODES;
			
			Counter innerBlockRounds = context.getCounter(PageRankEnum.INNER_BLOCK_ROUNDS);
			// Set up Maps for each pass of loop below
			HashMap<Integer, Node> nodesLastPass = new HashMap<Integer, Node>();
			HashMap<Integer, Node> nodesThisPass = new HashMap<Integer, Node>();

			double residualSum = Double.MAX_VALUE;
			
			// Each node is put into nodesLastPass for first pass
			for (Node n : nodes.values()){
				nodesLastPass.put(n.id, new Node(n));
			}
			double sum = 0;
			for (Node n: nodesLastPass.values())
				sum += n.getPR();
			System.out.println(" " + key + " INNER SUM: " + sum);
			// Run until converged in block
			int round = 0;
			double inBlockConstant = 1.;
			double expectedSum = 0;
			double nodesInBlock = nodes.size();
			while (residualSum/(double)nodesInBlock > CONST.RESIDUAL_SUM_DELTA){
				System.out.println("RESID: " + residualSum + " AVG: " + residualSum/nodes.size());
				residualSum = 0.;
				double newInBlockSink = 0;
				double sumInPr = 0.;
				// For each node from last pass
				double newRedistSum = 0.;
				double base_page_rank = CONST.BASE_PAGE_RANK;
				for (Node n : nodesLastPass.values()){
					
					// Base PR
					double pr = CONST.RANDOM_SURFER * base_page_rank + inBlockConstant * CONST.DAMPING_FACTOR * sinkPerNode;
					
					// Incoming PR added in
					
					
					if (outerEdges.containsKey(n.id))
						pr +=  CONST.DAMPING_FACTOR * outerEdges.get(n.id);
					
					
					// In Block PR added int
					if (innerEdges.containsKey(n.id)){
						ArrayList<Edge> ae = innerEdges.get(n.id);
						for (Edge e : ae){
							Node nn = nodesLastPass.get(e.from);
							pr += CONST.DAMPING_FACTOR * inBlockConstant * nn.prOnEdge();
							
						}
					}
					if (o.containsKey(n.id)){
						for (Edge e : o.get(n.id)){
							newRedistSum += pr/(double)n.edges(); 
						}
					} else if (n.edges() == 0){
						newInBlockSink += pr;
					}
					
					

					
					// Calculate Residual
					double residual = Math.abs((pr - n.getPR()))/pr;
					residualSum += residual;
					
					// Add node to a nodesThisPass since we have processed it
					Node nPrime = new Node(n);
					nPrime.setPR(pr);
					nodesThisPass.put(nPrime.id, nPrime);
					sumInPr += pr;

					
					
					
				}
				 sum = 0;
				 round++;
				for (Node nn: nodesThisPass.values())
					sum += nn.getPR();
				if (expectedSum == 0)
					expectedSum = sum;
				System.out.println(" " + key + " INNER SUM: " + sum);
				
				// Reset Holders for next round.
				// nodesThisPass becomes lastPass
				nodesLastPass = nodesThisPass;
				// Reset NodesThisPass
				nodesThisPass = new HashMap<Integer, Node>();

				inBlockConstant = (expectedSum - totalIncoming)/ (sinkPerNode * nodesInBlock + sumInPr - newRedistSum - newInBlockSink );
				base_page_rank = inBlockConstant * (sinkPerNode * nodesInBlock + sumInPr - newRedistSum - newInBlockSink )/nodesInBlock;
				// Check if we have converged
				//System.out.println(key + " " + residualSum);
				
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
			ByteBuffer block = Util.blockToByteBuffer(nodesLastPass, innerEdges, o);
			try {
				context.write(key, new BytesWritable(block.array()));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	

}
