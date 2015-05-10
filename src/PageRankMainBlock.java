
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;



public class PageRankMainBlock {
	public static void main(String[] args) throws Exception {
		Context c = new Context();
		BlockMapperPass0 pmr = new BlockMapperPass0();
		BlockReducerPass0 prr = new BlockReducerPass0();
		BufferedReader br = new BufferedReader(new FileReader("edges_no_spaces_3.txt"));
		String line = br.readLine();
		while(line != null && Util.idToBlock(Integer.parseInt(line.split(" ")[1])) < 10){
			pmr.map(new LongWritable(0), new Text(line), c);
			line = br.readLine();
		}
		br.close();
		for (Entry<LongWritable, ArrayList<Text>> m : c.mapData.entrySet())
			prr.reduce(m.getKey(), m.getValue(), c);
		int round = 0;
		double resid = 1.;
		double sum = 0;
		int counter = 0;
		for (Entry<LongWritable, Text> e : c.reduceData){
			Text v = e.getValue();
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			Util.fillMapsFromBlockString(v.toString().split(CONST.L0_DIV), nodes, null, null);
			int p = 0;
			for (Node n : nodes.values()){

				sum  += n.getPR();
				counter ++;
				p++;
			}
		}

		System.out.println("SUM: " + sum);
		System.out.println("COUNT: " + counter + " of " + CONST.TOTAL_NODES);
		while (resid > .01){
			PageRankBlockMapper pmrN = new PageRankBlockMapper();
			PageRankBlockReducer prrN = new PageRankBlockReducer();
			Context c2 = new Context();
			c2.getCounter(PageRankEnum.TOTAL_NODES).setValue(c.getCounter(PageRankEnum.TOTAL_NODES).getValue());
			c2.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).setValue(0);
			for (Entry<LongWritable, BytesWritable> r : c.reduceDatab)
				pmrN.map(r.getKey(), r.getValue(), c2);
			
			for (Entry<LongWritable, ArrayList<BytesWritable>> m : c2.mapDatab.entrySet())
				prrN.reduce(m.getKey(), m.getValue(), c2);
			c = c2;
			round++;
			sum = 0;
			counter = 0;
			for (Entry<LongWritable, BytesWritable> e : c.reduceDatab){
				
				HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
				HashMap<Integer, ArrayList<Edge>> i = new HashMap<Integer, ArrayList<Edge>>();

				HashMap<Integer, ArrayList<Edge>> o = new HashMap<Integer, ArrayList<Edge>>();
				Util.fillBlockFromByteBuffer(ByteBuffer.wrap(e.getValue().getBytes()), nodes, i, o);
				int p = 0;
				for (Node n : nodes.values()){

					sum += n.getPR();
					counter++;
					p++;
				}
			}

			System.out.println("SUM: " + sum);
			System.out.println("COUNT: " + counter + " of " + CONST.TOTAL_NODES);
			System.out.println("sum: " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
			System.out.println(" avg " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/(CONST.SIG_FIG_FOR_DOUBLE_TO_LONG * CONST.TOTAL_NODES));

			resid = c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;
		}
		sum = 0;
		counter = 0;
		for (Entry<LongWritable, Text> e : c.reduceData){
			Text v = e.getValue();
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			Util.fillMapsFromBlockString(v.toString().split(CONST.L0_DIV), nodes, null, null);
			int p = 0;
			for (Node n : nodes.values()){

				sum += n.getPR();
				counter++;
				p++;
			}
		}

		System.out.println("SUM: " + sum);
		System.out.println("COUNT: " + counter + " of " + CONST.TOTAL_NODES);
		System.out.println("outer rounds: " + round);

		
	}
}

