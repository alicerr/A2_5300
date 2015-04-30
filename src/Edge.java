import org.apache.hadoop.io.Text;


public class Edge {
	public final int from;
	public final int to;
	public final boolean inBlock;
	
	public Edge(int from, int to){
		this.from = from;
		this.to = to;
		this.inBlock = Util.idToBlock(from) == Util.idToBlock(to);
	}
	public Edge(int from, int to, boolean inBlock){
		this.from = from;
		this.to = to;
		this.inBlock = inBlock;
	}
	public Edge(Node from, Node to){
		this.from = from.id;
		this.to = to.id;
		inBlock = true;
		from.addBranch();
	}
	public Edge(String edge, boolean inBlock){
		String[] info = edge.split(CONST.L2_DIV);
		this.from = Integer.parseInt(info[CONST.EDGE_FROM]);
		this.to = Integer.parseInt(info[CONST.EDGE_TO]);
		this.inBlock = inBlock;
	}
	public Edge(Node from, int to){
		this.from = from.id;
		this.to = to;
		inBlock = false;
		from.addBranch();
	}
	public String toString(){
		return from + CONST.L2_DIV + to;
	}
	public Text toText(){
		return new Text(toString());
	}
}
