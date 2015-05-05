import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class OptimusReducer extends Reducer<ByteWritable, BytesWritable, ByteWritable, BytesWritable> {
	public void reduce(ByteWritable key, Iterable<BytesWritable> vals, Context context){
		//information holders for vals
		int numNodes = Util.numNodesInBlock(key.get());
		int base = Util.baseValue(key.get());
		double[] nodePR = new double[numNodes];
		int[] edgeCount = null;
		int[] inner = null;
		int[] outer = null;
		double inBlockSink = 0.;
		int count = 0;
		for (@SuppressWarnings("unused") BytesWritable val : vals)
			count++;
		double[] incomingVals  = new double[count - 1];
		int[] incomingNodes = new int[count - 1];
		int incomingIndex = 0;
		//get values passed into function
		for (BytesWritable val : vals){
			
			//block data
			if (val.getBytes().length == 11){
				ByteBuffer b = ByteBuffer.wrap(val.getBytes());
				incomingVals[incomingIndex] = b.getDouble();
				incomingNodes[incomingIndex++] = Util.get3BytesAsInt(new byte[]{b.get(), b.get(), b.get()});
			} 
			//incoming edge data
			else {
				int[][] info = Util.getBlock(val.getBytes(), nodePR, base);
				edgeCount = info[0];
				inner = info[1];
				outer = info[2];
			}
		}
		for (int i = 0; i < edgeCount.length; i++){
			if (edgeCount[i] == 0){
				inBlockSink += nodePR[i];
			}
		}
		
		//general variables from counters and constants
		double outOfBlockSink = CONST.DAMPING_FACTOR*((context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue() + .5)/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG - inBlockSink);
		double totalNodes = CONST.TOTAL_NODES;
		double basePageAddition = CONST.RANDOM_SURFER * CONST.BASE_PAGE_RANK + outOfBlockSink/totalNodes;
		
		//per round holders
		double nodesLastPass[] = new double[numNodes];
		double nodesThisPass[] = new double[numNodes];
		boolean converged = false;
		double residualSum = 0.;
		double newInBlockSink = 0.;
		for(int i = 0; i < numNodes; i++)
			nodesLastPass[i] = nodePR[i];
		int round = 0;
		//run convergence
		while (!converged){
			double baseInBlockPageAddition = CONST.DAMPING_FACTOR * (inBlockSink/(double)totalNodes);
			Arrays.fill(nodesThisPass, basePageAddition + baseInBlockPageAddition);
			for (int i = 0; i < incomingNodes.length; i++)
				nodesThisPass[incomingNodes[i] - base] += incomingVals[i];
			int innerEdgeIndex = 0;
			for (int i = 0; i < numNodes; i++){
				
				//base pr
			
				while(inner.length > innerEdgeIndex && inner[innerEdgeIndex] == i + base){
					int from = inner[innerEdgeIndex + 1];
					
					if (from < i + base)
						nodesThisPass[i] += nodesThisPass[from - base]/(double)edgeCount[from - base];
					else 
						nodesThisPass[i] += nodesLastPass[from - base]/(double)edgeCount[from - base];
					innerEdgeIndex += 2;
				}
				
				//residual
				
				residualSum += Math.abs((nodesThisPass[i] - nodesLastPass[i]))/nodesThisPass[i];
				
				
				
				//look for sink
				if (edgeCount[i] == 0)
					newInBlockSink += nodesThisPass[i];
				
			}
			//reset holders
			double[] hold = nodesLastPass;
			nodesLastPass = nodesThisPass;
			nodesThisPass = hold;
			inBlockSink = newInBlockSink;
			newInBlockSink = 0;
			
			//check for convergence
			converged = residualSum < CONST.RESIDUAL_SUM_DELTA;
			//System.out.println(key + " " + residualSum);
			residualSum = 0;
			round++;
		}
		
		//get residual from values passed into reducer
		double residualSumOuter = 0.;
		for (int i = 0; i < numNodes; i++){
			residualSumOuter += Math.abs(nodesLastPass[i] - nodePR[i])/nodesLastPass[i];
		}
		context.getCounter(PageRankEnum.RESIDUAL_SUM).increment((long) (residualSumOuter * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG));
		
		//save updated block
		try {
			context.write(key, new BytesWritable(Util.getBlockDataAsBytes(nodesLastPass, inner, outer)));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (key.get() == 0)
			System.out.println("Block Rounds: " + round);
	}
	

}
