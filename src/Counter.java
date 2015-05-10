
public class Counter {
	private long val = 0;
	public long getValue(){
		return val;
	}
	public void setValue(long v){
		val = v;
	}
	public void increment(long v){
		val += v;
	}
	public String toString(){
		return Long.toString(val);
	}
	
}
