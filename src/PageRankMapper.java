

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	public void mapper(IntWritable keyin, Text val, Context context){
		long pass = context.getCounter(PageRankEnum.PASS).getValue();
		if (pass == 0){
			String[] info = val.toString().split(" ");
			try {
				double select = Double.parseDouble(info[0]);
				IntWritable fromInt = new IntWritable(Integer.parseInt(info[1]));
				IntWritable toInt = new IntWritable(Integer.parseInt(info[2]));
				Text toText = new Text(info[2]);
				Text nullTo = new Text("-1");
				if (Util.retainEdgeByNodeID(select)){
					context.write(fromInt, toText);
				} else {
					context.write(fromInt, nullTo);
				}
				context.write(toInt, nullTo);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} else {
			String[] info = val.toString().split("|");
			String[] toList = info[0].split(",");
			Double pr = Double.parseDouble(info[1]);
			if (toList.length == 0){
				context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment(
						(long)(pr * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5)
						);
			} else {
				for (String to : toList){
					int toID = Integer.parseInt(to);
					try {
						context.write(new IntWritable(toID), new Text(Double.toString(pr/toList.length)));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
							
				}
			}
			try {
				context.write(keyin, new Text(info[0] + "|" + Double.toString(pr)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
