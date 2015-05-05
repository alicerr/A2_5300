import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class OptimusReducer0 extends Reducer<ByteWritable, BytesWritable, ByteWritable, BytesWritable> {
	public void reduce(ByteWritable key, Iterable<BytesWritable> vals, Context context){
		double[] pr = new double[Util.numNodesInBlock(key.get())];
		for (int i = 0; i < pr.length; i++) pr[i] = CONST.BASE_PAGE_RANK;
		ArrayList<int[]> innerEdges = new ArrayList<int[]>();
		ArrayList<int[]> outerEdges = new ArrayList<int[]>();
		int[] edgeCount = new int[pr.length];
		int base = Util.baseValue(key.get());

		for (BytesWritable val : vals){
			int[] info = Util.i3byteArrayToIntArray(val.getBytes());
			int from = info[0];
			int to = info[1];
			edgeCount[from - base]++; 
			if (Util.idToBlock(to) == key.get()) {
				innerEdges.add(new int[]{to, from});
			} else {
				outerEdges.add(new int[]{to, from});
			}
			
			
		}
		
		int[] inner =  Util.sortAndFlatten(innerEdges);
		int[] outer =  Util.sortAndFlatten(outerEdges);
		try {
			context.write(key, new BytesWritable(Util.getBlockDataAsBytes(pr, inner, outer)));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		 
	}

}
