import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class OptimusMapper1 extends Mapper<ByteWritable, BytesWritable, ByteWritable, BytesWritable>  {
	
	public void map(ByteWritable key, BytesWritable val, Context context){
		
		
		int base = Util.baseValue(key.get());
		int nodeCount = Util.numNodesInBlock(key.get());
		double[] nodes = new double[nodeCount];
		int[][] info = Util.getBlock(val.getBytes(), nodes, base);
		int[] edgeCount = info[0];
		int[] outer = info[2];

		double sinks = 0;
		for (int i = 0; i < edgeCount.length; i++)
			if (edgeCount[i] == 0)
				sinks += nodes[i];
		
		context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment((long) (sinks * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
		
		for (int i = 0; i < outer.length; i += 2){
			int to = outer[i];
			int from = outer[i + 1];
			ByteBuffer b = ByteBuffer.allocate(11);
			b.putDouble(nodes[from - base]/(double)edgeCount[from - base]);
			byte[] tob = Util.getIntAs3Bytes(to);
			for (byte tb : tob) b.put(tb);
			try {
				context.write(new ByteWritable((byte)Util.idToBlock(to)), new BytesWritable(b.array()));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		try {
			context.write(key, val);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
}
	

