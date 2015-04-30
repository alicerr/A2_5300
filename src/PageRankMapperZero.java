import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapperZero extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	
	public void mapper(LongWritable keyin, Text val, Context context){
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
	}
}
