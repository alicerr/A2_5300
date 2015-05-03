import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GetFinalNodesMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
public void map(LongWritable keyin, Text val, Context context){
		

		String[] info = val.toString().split(CONST.L0_DIV, -1);
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		Util.fillMapsFromBlockString(info, nodes, null, null);

		
		

		for (Node n : nodes.values()){
			if (Util.isInFirstTwoIndex((int) keyin.get(), n.id)){
				try {
					context.write(new LongWritable(n.id), new Text(n.getPR() + ""));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	
	}
}
