import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducerZero extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> vals, Context context){
		String toList = "";
		for (Text val : vals){
			if (!val.toString().equals("-1"))
				toList += "," + val.toString();
		}
		if (toList.length() > 0){
			toList = toList.substring(1);
		} 
		try {
			context.write(key, new Text(toList + "|" + CONST.BASE_PAGE_RANK));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		context.getCounter(PageRankEnum.TOTAL_NODES).increment(1);
	}
}
