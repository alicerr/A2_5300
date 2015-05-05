import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class OptimusMapper0 extends Mapper<LongWritable, Text, ByteWritable, BytesWritable> {
	
	public void map(LongWritable keyin, Text val, Context context){
		
		String[] info = val.toString().split(" ");
	
		try {
			double select = Double.parseDouble(info[0]);
			if (Util.retainEdgeByNodeID(select)){
				int fromInt = Integer.parseInt(info[1]);
				int toInt = Integer.parseInt(info[2]);
				byte fromBlock = (byte) Util.idToBlock(fromInt);
				try {
					context.write(new ByteWritable(fromBlock), new BytesWritable(Util.intArrayToi3Bytes(new int[]{fromInt, toInt})));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				} 
			}catch (NumberFormatException e) {
			e.printStackTrace();
		}
			
	}
}