import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;



public class PageRankMain {
	public static void main(String[] args) throws Exception {
		Context c = new Context();
		PageRankMapperZero pmr = new PageRankMapperZero();
		PageRankReducerZero prr = new PageRankReducerZero();
		BufferedReader br = new BufferedReader(new FileReader("edges_no_spaces_3.txt"));
		String line = br.readLine();
		double sum = 0.;
		int counter = 0;
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
		while(line != null){
			pmr.map(new LongWritable(0), new Text(line), c);
			line = br.readLine();
		}
		br.close();
		for (Entry<LongWritable, ArrayList<Text>> m : c.mapData.entrySet())
			prr.reduce(m.getKey(), m.getValue(), c);
		int round = 0;
		while (round < 5){
			PageRankMapper pmrN = new PageRankMapper();
			PageRankReducer prrN = new PageRankReducer();
			Context c2 = new Context();
			c2.getCounter(PageRankEnum.TOTAL_NODES).setValue(c.getCounter(PageRankEnum.TOTAL_NODES).getValue());
			c2.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).setValue(0);
			for (Entry<LongWritable, Text> r : c.reduceData)
				pmrN.map(r.getKey(), r.getValue(), c2);
			
			for (Entry<LongWritable, ArrayList<Text>> m : c2.mapData.entrySet())
				prrN.reduce(m.getKey(), m.getValue(), c2);
			c = c2;
					round++;
		System.out.println("sum: " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
		System.out.println(" avg " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/(CONST.SIG_FIG_FOR_DOUBLE_TO_LONG * c.getCounter(PageRankEnum.TOTAL_NODES).getValue()));
		System.out.println("0: " + c.reduceData.get(0).getKey().get() + " " + c.reduceData.get(0).getValue());
			
		}
		sum = 0.;
		counter = 0;
		for (Entry<LongWritable, Text> e : c.reduceData){
			Text v = e.getValue();
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			double pr = Double.parseDouble(v.toString().split(CONST.L0_DIV, -1)[1]);
			int p = 0;
			sum += pr;
			
		}
		System.out.println("SUM: " + sum);
		System.out.println("COUNT: " + counter + " of " + CONST.TOTAL_NODES);
		while (round < 50){
			PageRankMapper pmrN = new PageRankMapper();
			PageRankReducer prrN = new PageRankReducer();
			Context c2 = new Context();
			c2.getCounter(PageRankEnum.TOTAL_NODES).setValue(c.getCounter(PageRankEnum.TOTAL_NODES).getValue());
			c2.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).setValue(0);
			for (Entry<LongWritable, Text> r : c.reduceData)
				pmrN.map(r.getKey(), r.getValue(), c2);
			
			for (Entry<LongWritable, ArrayList<Text>> m : c2.mapData.entrySet())
				prrN.reduce(m.getKey(), m.getValue(), c2);
			c = c2;
					round++;
					
		System.out.println("sum: " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
		System.out.println(" avg " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/(CONST.SIG_FIG_FOR_DOUBLE_TO_LONG * c.getCounter(PageRankEnum.TOTAL_NODES).getValue()));

			
		}
		sum = 0.;
		counter = 0;
		for (Entry<LongWritable, Text> e : c.reduceData){
			Text v = e.getValue();
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			double pr = Double.parseDouble(v.toString().split(CONST.L0_DIV, -1)[1]);
			int p = 0;
			sum += pr;
			
		}
		
		System.out.println("SUM: " + sum);
		System.out.println("COUNT: " + counter + " of " + CONST.TOTAL_NODES);

		
	}
}
