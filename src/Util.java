import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;


public abstract class Util {

	public static boolean retainEdgeByNodeID(double x){
		// compute filter parameters for netid shs257
		double fromNetID = 0.752;// 82 is 28 reversed
		double rejectMin = 0.9 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
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
	
	public static byte[] getIntAs3Bytes(int i){
		byte b3 = (byte)(i & 255);
		byte b2 = (byte)((i >> 8) & 255);
		byte b1 = (byte)((i >> 16) & 255);
		return new byte[]{b1, b2, b3 };
	}
	public static int get3BytesAsInt(byte[] b){
		return ByteBuffer.wrap(new byte[]{0, b[0], b[1], b[2]}).getInt();
	}
	
	public static byte[] intArrayToi3Bytes(int[] a){
		ByteBuffer b = ByteBuffer.allocate((int) (a.length * 3));
		for (int i = 0; i < a.length; i++){
		
			
			b.put((byte) ((a[i] >>> 16) & 255));
			b.put((byte) ((a[i] >>> 8) & 255));		
			b.put((byte) (a[i] & 255));
			}
		 return b.array();
		}
	public static  int[] i3byteArrayToIntArray(byte[] b){
		int[] i = new int[b.length/3];
		int hold = 0;
		int iIndex = 0;
		for (int x = 0; x < b.length; x++){
			if (x % 3 == 0 && x != 0){
				i[iIndex++] = hold;
				hold = 0;
			}
			hold = hold << 8;
			hold = hold + (b[x] & 255);
		}
		i[iIndex] = hold;
		return i;
	}
	public static  int[] i3byteArrayToIntArray(byte[] b, int start, int end){
		int[] i = new int[(end - start)/3];
		int hold = 0;
		int iIndex = 0;
		for (int x = start; x < end; x++){

			hold = hold << 8;
			hold = hold + (b[x] & 255);
			if (x % 3 == 2){
				i[iIndex++] = hold;
				hold = 0;
			}
		}
		return i;
	}
	
	public static int numNodesInBlock(int id){
		if (id == 0)
			return blocks[id];
		else
			return blocks[id] - blocks[id -1];
	}

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
						outerEdges.put(e.to, outerEdgesFromNode);
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
	public static ByteBuffer blockToByteBuffer(HashMap<Integer, Node> n, HashMap<Integer, ArrayList<Edge>> i, HashMap<Integer, ArrayList<Edge>> o){
		int innerEdgeCount = 0;
		for (ArrayList<Edge> ae: i.values())
				innerEdgeCount += ae.size();
		
		int outerEdgeCount = 0;
		for (ArrayList<Edge> ae: o.values()){
			outerEdgeCount += ae.size();
		}
		ByteBuffer b = ByteBuffer.allocate(1 + 4+ (4+4+8)*n.size() + 8 * innerEdgeCount + 8 * outerEdgeCount + 8);
		b.put(CONST.ENTIRE_BLOCK_DATA_MARKER);
		b.putInt(n.size());
		for (Node nn : n.values()){
			b.putInt(nn.id);
			b.putInt(nn.edges());
			b.putDouble(nn.getPR());
			
		}
		b.putInt(innerEdgeCount);
		for (ArrayList<Edge> ae: i.values()){
			for (Edge e: ae){
				b.putInt(e.from);
				b.putInt(e.to);
			}
		}
		b.putInt(outerEdgeCount);
		for (ArrayList<Edge> ae: o.values()){
			for (Edge e: ae){
				b.putInt(e.from);
				b.putInt(e.to);
			}
		}
		return b;
	}
	public static double fillBlockFromByteBuffer(ByteBuffer b, HashMap<Integer, Node> n, HashMap<Integer, ArrayList<Edge>> i, HashMap<Integer, ArrayList<Edge>> o){
		b.get();
		int nodeCount = b.getInt();
		double sink = 0.;
		for (int ind = 0; ind < nodeCount; ind++){
			Node nn = new Node(b.getInt());
			nn.setEdges(b.getInt());
			nn.setPR(b.getDouble());
			n.put(nn.id, nn);
			if (nn.edges() == 0){
				sink+=nn.getPR();
			}
		}
		int iCount = b.getInt();
		for (int ind = 0; ind < iCount; ind++){
			Edge e = new Edge(b.getInt(), b.getInt());
			if (i.containsKey(e.to))
				i.get(e.to).add(e);
			else{
				ArrayList<Edge> ae = new ArrayList<Edge>();
				ae.add(e);
				i.put(e.to,  ae);
			}
		}
		int oCount = b.getInt();
		if (o != null){
			for (int ind = 0; ind < oCount; ind++){
				Edge e = new Edge(b.getInt(), b.getInt());
				if (o.containsKey(e.from))
					o.get(e.from).add(e);
				else{
					ArrayList<Edge> ae = new ArrayList<Edge>();
					ae.add(e);
					o.put(e.from,  ae);
				}
			}
		}
		
		return sink;
	}
	public static byte[] incomingValue(int to, Node n){
		ByteBuffer b = ByteBuffer.allocate(13);
		b.put(CONST.INCOMING_EDGE_MARKER);
		b.putInt(to);
		b.putDouble(n.prOnEdge());
		return b.array();
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

	public static int baseValue(byte b) {
		return  b == 0 ? 0 : blocks[b - 1];
	}

	public static byte[] getBlockDataAsBytes(double[] pr,
			int[] innerEdges, int[] outerEdges) {
		ByteBuffer b = ByteBuffer.allocate(pr.length * 8 + 4 + innerEdges.length * 3 + outerEdges.length * 3);
		for (int i = 0; i < pr.length; i++)
			b.putDouble(pr[i]);
		b.putInt(innerEdges.length);
		for (int i = 0; i < innerEdges.length; i++){
			byte[] ib = getIntAs3Bytes(innerEdges[i]);
			for (byte bb : ib)
				b.put(bb);
		}
		for (int i = 0; i < outerEdges.length; i++){
			byte[] ib = getIntAs3Bytes(outerEdges[i]);
			for (byte bb : ib)
				b.put(bb);
		}
		System.out.println(b.array().length);
		System.out.println(b.position());
		return b.array();
			
	}
	public static int[][] getBlock(byte[] b, double[] nodes, int base){
		ByteBuffer bb = ByteBuffer.wrap(b);
		for (int i = 0; i < nodes.length; i++){
			nodes[i] = bb.getDouble();
		}
		int numInnerEdge = bb.getInt();
		int[] inner = new int[numInnerEdge];
		int[] edges = new int[nodes.length];
		
			for (int i = 0; i < inner.length; i++){
				byte[] bbb = new byte[]{bb.get(), bb.get(), bb.get()};
				int e = get3BytesAsInt(bbb);
			inner[i] = e;
			try{			
				if (i % 2 == 1)
					edges[e - base]++;
				
			} catch (Exception ee){
				System.out.println(ee);
				System.out.println("e:" + e);
				System.out.println("base:" + base);
				System.out.println("i:" + i);
				for (byte bbbb : bbb){
					System.out.println(bbbb);
				}
				for (int ii = nodes.length * 8; ii < b.length; ii++){
					System.out.print(ii + ":" + b[ii] + ",");
				}
				System.out.println("to:" + inner[i - 1]);
				throw ee;
			}
			}
		int bIndex = bb.position();
		int[] outer = new int[(b.length - bIndex)/3];
		for (int i = 0; i < outer.length; i++){
			byte[] bbb = new byte[]{bb.get(), bb.get(), bb.get()};
			int e = get3BytesAsInt(bbb);
			outer[i] = e;
			if (i % 2 == 1)
				try{
				edges[e - base]++;
				} catch (Exception ee){
					System.out.println(bb.position());
					System.out.println(ee);
					System.out.println("e:" + e);
					System.out.println("base:" + base);
					System.out.println("i:" + i);
					System.out.println("to:" + outer[i - 1]);
					for (byte bbbb : bbb){
						System.out.println(bbbb);
					}
					for (int ii = nodes.length * 8   + 4 +inner.length * 3; ii < b.length; ii++){
						System.out.print(ii + ":" + b[ii] + ",");
					}
					
					throw ee;
				}
				}
		return new int[][]{edges, inner, outer};
				
				
	}

	public static int[] sortAndFlatten(ArrayList<int[]> edges) {
		 Collections.sort(edges,new Comparator<int[]>() {
	            public int compare(int[] edge1, int[] edge2) {
	                return Integer.compare(edge1[0], edge2[0]);
	            }
	        });
		 
		 int[][] ee = (int[][]) edges.toArray();
		 int[] e = new int[ee.length * 2];
		 
		 for(int ii = 0; ii < e.length;){
			 e[ii] = ee[ii/2][0];
			 e[ii] = ee[ii/2][1];
		 }
		 System.out.println(Arrays.toString(e));
		 return e;
	}

	public static boolean isInFirstTwoIndex(int i, int id) {
		return (i == 0 && id < 2) || (i > 0 && id < blocks[i-1] + 2);
	}
	
	public boolean BlockEq(HashMap<Integer, Node> n1, HashMap<Integer, Node> n2, HashMap<Integer, ArrayList<Edge>> i1,  HashMap<Integer, ArrayList<Edge>> i2,  HashMap<Integer, ArrayList<Edge>> o1,  HashMap<Integer, ArrayList<Edge>> o2){
		for (Node n : n1.values())
			if (!(n2.containsKey(n.id) && n2.get(n.id).equals(n)))
				return false;
		for (Node n : n2.values())
			if (!(n1.containsKey(n.id) && n1.get(n.id).equals(n)))
				return false;
		for (ArrayList<Edge> ae : i1.values())
			for (Edge e : ae)
				if (!(i2.containsKey(e.to) && i2.get(e.to).contains(e)))
					return false;
		for (ArrayList<Edge> ae : i2.values())
			for (Edge e : ae)
				if (!(i1.containsKey(e.to) && i1.get(e.to).contains(e)))
					return false;
		for (ArrayList<Edge> ae : o1.values())
			for (Edge e : ae)
				if (!(o2.containsKey(e.to) && o2.get(e.to).contains(e)))
					return false;
		for (ArrayList<Edge> ae : o2.values())
			for (Edge e : ae)
				if (!(o2.containsKey(e.to) && o2.get(e.to).contains(e)))
					return false;
		return true;
	}
	
	
	
}
