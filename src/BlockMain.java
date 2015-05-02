
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
public class BlockMain {
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String outputFile = args[1] + " pass 0";
	    
	    Job job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
	    
	    job.setJarByClass(BlockMain.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    
	    job.setMapperClass(BlockMapperPass0.class);
	    job.setReducerClass(BlockReducerPass0.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);     
        job.waitForCompletion(true);
        long totalNodes = job.getCounters().findCounter(PageRankEnum.TOTAL_NODES).getValue();
        int round = 1;
        double residualSum = 1;
        while (residualSum > CONST.DELTA){
        	String inputFile = outputFile;
        	outputFile = args[1] + " pass " + round;
        	conf = new Configuration();
        	conf.setLong("TOTAL_NODES", totalNodes);
        	job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
        	FileInputFormat.setInputPaths(job, new Path(inputFile));
     	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
     	    
     	    job.setMapperClass(PageRankBlockMapper.class);
     	    job.setReducerClass(PageRankBlockReducer.class);
     	    job.setOutputKeyClass(LongWritable.class);
     	    job.setOutputValueClass(Text.class);
             
     	    job.setInputFormatClass(SequenceFileInputFormat.class);
     	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
             
            job.waitForCompletion(true);
            residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;
        	round++;
        }
	    

	   
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	}

        
}
