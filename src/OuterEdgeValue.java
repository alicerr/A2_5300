
public class OuterEdgeValue {
	public final int to;
	public final double pr;
	
	public OuterEdgeValue(int to, double pr){
		this.to = to;
		this.pr = pr;
	}

	public OuterEdgeValue(String[] info) {
		this.to = Integer.parseInt(info[1]);
		this.pr = Double.parseDouble(info[2]);
		
	}
}
