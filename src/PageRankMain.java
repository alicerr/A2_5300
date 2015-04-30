
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class PageRankMain {
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
	    job.setJarByClass(PageRankMain.class);
	    job.setMapperClass(PageRankMapperZero.class);
	    job.setReducerClass(PageRankReducerZero.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    Path in = new Path(args[0]);
        Path out = new Path(args[1] + " pass 0");
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        FileSystem fs = FileSystem.get(out.toUri(), conf);//FileSystem.get(conf);
        if (fs.exists(out))
                fs.delete(out, true);
	    //FileInputFormat.addInputPath(job, new Path(args[0]));
	    //FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    //System.exit(job.waitForCompletion(true) ? 0 : 1);
        SequenceFileOutputFormat.setOutputPath(job, out);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        System.out.println("Running first Job..");
        job.waitForCompletion(true);
        System.out.println("First Job Complete..");


	    Counter residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM);
	    job.getCounters().findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).setValue(0);
	    Counter totalNodes = job.getCounters().findCounter(PageRankEnum.TOTAL_NODES);
	    // counter from the previous running import job
	    int passes = 0;
	    int round = 0;
	    job.waitForCompletion(true);
	    round++;
	    while (round < 6) {
	    	System.out.println("Entering Loop. Round: " + round);
	    	conf = new Configuration();
	    	job = Job.getInstance(conf, "page rank " + args[1] + " pass " + passes);
		    job.setMapperClass(PageRankMapper.class);
		    job.setReducerClass(PageRankReducer.class);
		    job.setOutputKeyClass(IntWritable.class);	
		    job.setJarByClass(PageRankMain.class);
		    job.setOutputValueClass(Text.class);
		    passes = round;
		    residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM);
		    residualSum.setValue(0);
		    job.getCounters().findCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).setValue(0);;
		    job.getCounters().findCounter(PageRankEnum.TOTAL_NODES).setValue(totalNodes.getValue());
		    
		    // always work on the path of the previous depth
		    in = new Path(args[1] + " pass " + (passes - 1));
		    out = new Path(args[1] + " pass " + passes);

		    SequenceFileInputFormat.addInputPath(job, in);
		    // delete the outputpath if already exists
		    if (fs.exists(out))
		    	fs.delete(out, true);
		    
		    System.out.println("Middle Round:" + round);
		    SequenceFileOutputFormat.setOutputPath(job, out);
		    job.setInputFormatClass(SequenceFileInputFormat.class);
		    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    // wait for completion and update the counter
		    System.out.println("Running "+passes+" Loop Job...");
	        job.waitForCompletion(true);
	        System.out.println("round: " + round + "\nresidual: " + residualSum.getValue()/(double)(totalNodes.getValue() * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG));
	        round++;
	      }
	  }
}
