import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;


public abstract class Util {

	public static boolean retainEdgeByNodeID(double number){
		// compute filter parameters for netid shs257
		double fromNetID = 0.752; // 752 is 257 reversed
		double rejectMin = 0.9 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		return (number < rejectMin || number >= rejectLimit);
	}
	
	public static int idToBlock(int id){
		int guess = id/10078;
		boolean goodGuess = false;
		while (!goodGuess){
			if (guess + 1 < blocks.length && blocks[guess] <= id){
				guess++;
			} else if (guess > 0 && blocks[guess - 1] > id) {
				guess--;
			} else {
				goodGuess = true;
			}
		}
		//System.out.println(id +  " is in block " + guess + " and between " + blocks[guess] + " and " + blocks[guess] + (id < blocks[guess] && (guess > 0 ? id > blocks[guess - 1] : true)));
		return guess;
	}
	
	
	public final static int[] blocks  = {
		10328,
		20373,
		30629,
		40645,
		50462,
		60841,
		70591,
		80118,
		90497,
		100501,
		110567,
		120945,
		130999,
		140574,
		150953,
		161332,
		171154,
		181514,
		191625,
		202004,
		212383,
		222762,
		232593,
		242878,
		252938,
		263149,
		273210,
		283473,
		293255,
		303043,
		313370,
		323522,
		333883,
		343663,
		353645,
		363929,
		374236,
		384554,
		394929,
		404712,
		414617,
		424747,
		434707,
		444489,
		454285,
		464398,
		474196,
		484050,
		493968,
		503752,
		514131,
		524510,
		534709,
		545088,
		555467,
		565846,
		576225,
		586604,
		596585,
		606367,
		616148,
		626448,
		636240,
		646022,
		655804,
		665666,
		675448,
		685230};


	public static String getBlockDataAsString(StringBuffer nodes,
			StringBuffer innerEdges, StringBuffer outerEdges) {
		if (nodes.length() == 0)
			nodes.append(CONST.L1_DIV);
		if (innerEdges.length() == 0)
			innerEdges.append(CONST.L1_DIV);
		if (outerEdges.length() == 0)
			outerEdges.append(CONST.L1_DIV);
		
		return CONST.ENTIRE_BLOCK_DATA_MARKER + CONST.L0_DIV +
			   nodes.toString().substring(1) + CONST.L0_DIV +
			   innerEdges.toString().substring(1) + CONST.L0_DIV +
			   outerEdges.toString().substring(1);
		
	}
	
	public static double fillMapsFromBlockString(String[] info, 
			HashMap<Integer, Node> nodes, 
			HashMap<Integer, ArrayList<Edge>> innerEdges, 
			HashMap<Integer, ArrayList<Edge>> outerEdges){

		String[] nodesString = info[CONST.NODE_LIST].split(CONST.L1_DIV, -1);
		String[] outerEdgesString = info[CONST.OUTER_EDGE_LIST].split(CONST.L1_DIV, -1);
		String[] innerEdgesString = info[CONST.INNER_EDGE_LIST].split(CONST.L1_DIV, -1);


		for (String nodeString : nodesString){
			if (!nodeString.equals("")){
				Node node = new Node(nodeString);
				nodes.put(node.id, node);
			}
		}
		for (String edgeString : innerEdgesString){
			if (!edgeString.equals("")){
				Edge e = new Edge(edgeString, true);
				nodes.get(e.from).addBranch();
	
				if (innerEdges != null){
					if (innerEdges.containsKey(e.to))
						innerEdges.get(e.to).add(e);
					else{
						ArrayList<Edge> edgesThatGoToNode = new ArrayList<Edge>();
						edgesThatGoToNode.add(e);
						innerEdges.put(e.to, edgesThatGoToNode);
					}
				}
			}
		}
		for (String edgeString : outerEdgesString){
			if (!edgeString.equals("")){
				Edge e = new Edge(edgeString, false);
				nodes.get(e.from).addBranch();
				
				if (outerEdges != null){
					if (outerEdges.containsKey(e.to)){
						outerEdges.get(e.to).add(e);
					} else {
						ArrayList<Edge> outerEdgesFromNode = new ArrayList<Edge>();
						outerEdgesFromNode.add(e);
						outerEdges.put(e.from, outerEdgesFromNode);
					}
				}
			}
				
		}
		double sinks = 0.;
		for (Node n : nodes.values()){
			if (n.edges() == 0)
				sinks += n.getPR();
		}
		return sinks;
	}

	public static String getBlockDataAsString(
			HashMap<Integer, Node> nodes,
			String innerEdgesString,
			String outerEdgesString) {
		StringBuffer nodesSB = new StringBuffer();
		for (Node n : nodes.values()){
			nodesSB.append(CONST.L1_DIV + n.toString());
		} 
		if (nodes.size() == 0)
			nodesSB.append(CONST.L1_DIV);
		return CONST.ENTIRE_BLOCK_DATA_MARKER + CONST.L0_DIV + nodesSB.toString().substring(1) + CONST.L0_DIV + innerEdgesString + CONST.L0_DIV + outerEdgesString;
	}
	
	
	
}
