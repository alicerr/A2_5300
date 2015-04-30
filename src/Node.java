import org.apache.hadoop.io.Text;




public class Node {
	public final int id;
	private double pr = 0;
	private int edges = 0;
	public Node(int id){
		this.id = id;
	}
	public Node(Node n){
		this.id = n.id;
		this.edges = n.edges;
		this.pr = n.pr;
	}
	public Node(String nodeAsText){
		String[] info = nodeAsText.split(CONST.L2_DIV);
		id = Integer.parseInt(info[0]);
		if (info.length > 1){
			pr = Double.parseDouble(info[1]);
		} 
	}
	public void incrementPR(double inc){
		this.pr += inc;
	}
	public void setPR(double set){
		this.pr = set;
	}
	public void addBranch(){
		edges++;
	}
	public double getPR(){
		return pr;
	}
	public double residual(Node oldNode){
		return Math.abs(pr - oldNode.pr)/pr;
	}
	public double prOnEdge(){
		if (edges > 0)
			return pr/(double)edges;
		else return 0.;
	}
	public String toString(){
		return id + CONST.L2_DIV + pr;
	}
	public Text toText(){
		return new Text(toString());
	}
	public int edges(){ return this.edges; }
	
	
}
