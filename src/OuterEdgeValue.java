
/**
 * OuterEdgeObject for PageRank
 * @author Alice, Spencer, Garth
 *
 */
public class OuterEdgeValue {
	public final int to;
	public final double pr;
	
	/**
	 * Creates OuterEdgeValue
	 * @param to nodeid
	 * @param pr pagerank value
	 */
	public OuterEdgeValue(int to, double pr){
		this.to = to;
		this.pr = pr;
	}

	/**
	 * Creates OuterEdgeValue from string array
	 * @param info string array
	 */
	public OuterEdgeValue(String[] info) {
		this.to = Integer.parseInt(info[1]);
		this.pr = Double.parseDouble(info[2]);
		
	}
}
