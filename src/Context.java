import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Context {
	public HashMap<PageRankEnum, Counter> counters = new HashMap<PageRankEnum, Counter>();
	public HashMap<LongWritable, ArrayList<Text>> mapData = new HashMap<LongWritable, ArrayList<Text>>();
	public ArrayList<Entry<LongWritable, Text>> reduceData = new ArrayList<Entry<LongWritable, Text>>();
	public HashMap<LongWritable, ArrayList<BytesWritable>> mapDatab = new HashMap<LongWritable, ArrayList<BytesWritable>>();
	public ArrayList<Entry<LongWritable, BytesWritable>> reduceDatab = new ArrayList<Entry<LongWritable, BytesWritable>>();

	public Counter getCounter(PageRankEnum e){
		if (counters.containsKey(e))
			return counters.get(e);
		else{
			Counter c = new Counter();
			counters.put(e, c);
			return c;
		}
	}
	public void write(LongWritable key, Text val)throws IOException, InterruptedException{
		String callingClass = KDebug.getCallerCallerClassName();
		if (callingClass.contains("apper")){
			if (mapData.containsKey(key)){
				mapData.get(key).add(val);
			} else {
				ArrayList<Text> t = new ArrayList<Text>();
				t.add(val);
				mapData.put(key, t);
			}
		} else {
			reduceData.add(new AbstractMap.SimpleEntry<LongWritable, Text>(key, val));
		}
	}
	public void write(LongWritable key, BytesWritable val) throws IOException, InterruptedException{
		String callingClass = KDebug.getCallerCallerClassName();
		if (callingClass.contains("apper")){
			if (mapDatab.containsKey(key)){
				mapDatab.get(key).add(val);
			} else {
				ArrayList<BytesWritable> t = new ArrayList<BytesWritable>();
				t.add(val);
				mapDatab.put(key, t);
			}
		} else {
			reduceDatab.add(new AbstractMap.SimpleEntry<LongWritable, BytesWritable>(key, val));
		}
		
	}

	
}
