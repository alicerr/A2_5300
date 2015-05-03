

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		double redistributeValue = context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue()/(context.getConfiguration().getLong("TOTAL_NODES", 685230) * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
		double newPageRank = CONST.RANDOM_SURFER*CONST.BASE_PAGE_RANK + redistributeValue;
		double oldPageRank = 0.;
		String toList = "";
		for (Text val : vals){
			if (val.toString().contains(CONST.L0_DIV)){
				String[] info = val.toString().split(CONST.L0_DIV);
				toList = info[0];
				oldPageRank = Double.parseDouble(info[1]);
			} else {
				newPageRank += CONST.DAMPING_FACTOR * Double.parseDouble(val.toString());
			}
			
		}
		double residualValue = Math.abs(newPageRank - oldPageRank)/newPageRank;
		long incrementValue = (long)(residualValue * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5);
		System.out.println("Increment Value: " + incrementValue);
		context.getCounter(PageRankEnum.RESIDUAL_SUM).increment(incrementValue);
		try {
			context.write(key, new Text(toList + CONST.L0_DIV + newPageRank + CONST.L0_DIV + residualValue));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
