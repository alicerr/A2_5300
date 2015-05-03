import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducerZero extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		String toList = "";
		for (Text val : vals){
			if (!val.toString().equals("-1"))
				toList += "," + val.toString();
		}
		if (toList.length() > 0){
			toList = toList.substring(1);
		} 
		try {
			context.write(key, new Text(toList + CONST.L0_DIV + CONST.BASE_PAGE_RANK));
		} catch (IOException | InterruptedException e) {
			System.out.println("error3");
			e.printStackTrace();
		}
		context.getCounter(PageRankEnum.TOTAL_NODES).increment(1);
	}
}
