import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapperZero extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	
	public void map(LongWritable keyin, Text val, Context context){
		String[] info = val.toString().split(" ");
		try {
			double select = Double.parseDouble(info[0]);
			LongWritable fromInt = new LongWritable(Integer.parseInt(info[1]));
			LongWritable toInt = new LongWritable(Integer.parseInt(info[2]));
			Text toText = new Text(info[2]);
			Text nullTo = new Text("-1");
			if (Util.retainEdgeByNodeID(select)){
				try {
					context.write(fromInt, toText);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					context.write(fromInt, nullTo);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				context.write(toInt, nullTo);
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
