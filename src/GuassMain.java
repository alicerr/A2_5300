
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;



public class GuassMain {
	public static void main(String[] args) throws Exception {
		Context c = new Context();
		BlockMapperPass0 pmr = new BlockMapperPass0();
		BlockReducerPass0 prr = new BlockReducerPass0();
		BufferedReader br = new BufferedReader(new FileReader("edges_no_spaces_3.txt"));
		String line = br.readLine();
		double sum = 0;
		int counter = 0;

		while(line != null){
			pmr.map(new LongWritable(0), new Text(line), c);
			line = br.readLine();
		}
		br.close();
		for (Entry<LongWritable, ArrayList<Text>> m : c.mapData.entrySet())
			prr.reduce(m.getKey(), m.getValue(), c);
		int round = 0;
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
		double residual = Double.MAX_VALUE;
		while (residual/CONST.TOTAL_NODES > CONST.RESIDUAL_SUM_DELTA){
			PageRankBlockMapper pmrN = new PageRankBlockMapper();
			GuassReducer prrN = new GuassReducer();
			Context c2 = new Context();
			for (Entry<LongWritable, Text> r : c.reduceData)
				pmrN.map(r.getKey(), r.getValue(), c2);
			
			
			for (Entry<LongWritable, ArrayList<Text>> m : c2.mapData.entrySet())
				prrN.reduce(m.getKey(), m.getValue(), c2);
			c = c2;
			round++;
			System.out.println("sum: " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
			System.out.println(" avg " + c.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/(CONST.SIG_FIG_FOR_DOUBLE_TO_LONG * CONST.TOTAL_NODES));
			System.out.println(" avg rounds " + c.getCounter(PageRankEnum.INNER_BLOCK_ROUNDS).getValue()/(CONST.SIG_FIG_FOR_DOUBLE_TO_LONG * CONST.TOTAL_NODES));
			
			residual = c2.getCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;
			sum = 0.;
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
		}
		System.out.println(round + "rounds");
		sum = 0;
		counter = 0;
		for (Entry<LongWritable, Text> e : c.reduceData){
			Text v = e.getValue();
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			Util.fillMapsFromBlockString(v.toString().split(CONST.L0_DIV), nodes, null, null);
			int p = 0;
			for (Node n : nodes.values()){
				if (p < 2){
					System.out.println("Node " + n.id + " PR " + n.getPR());
				}
				sum += n.getPR();
				counter++;
				p++;
			}
		}
		System.out.println("SUM: " + sum);
		System.out.println("COUNT: " + counter + " of " + CONST.TOTAL_NODES);
		
	}

}

