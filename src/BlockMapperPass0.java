import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlockMapperPass0 extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void mapper(LongWritable keyin, Text val, Context context){
		
		String[] info = val.toString().split(" ");
		try {
			double select = Double.parseDouble(info[0]);
			int fromInt = Integer.parseInt(info[1]);
			int toInt = Integer.parseInt(info[2]);
			int fromBlock = Util.idToBlock(fromInt);
			int toBlock = Util.idToBlock(toInt);
			
			
			if (Util.retainEdgeByNodeID(select)){
				try {
					context.write(new LongWritable(fromBlock), new Text(CONST.SEEN_EDGE_MARKER + CONST.L0_DIV + fromInt + CONST.L0_DIV + toInt));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					context.write(new LongWritable(fromBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + fromInt));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			try {
				context.write(new LongWritable(toBlock), new Text(CONST.SEEN_NODE_MARKER + CONST.L0_DIV + toInt));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
}