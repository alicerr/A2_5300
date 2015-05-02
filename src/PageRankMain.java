
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class PageRankMain {
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String outputFile = args[1] + " pass 0";
	    
	    Job job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
	    job.setJarByClass(PageRankMain.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    job.setMapperClass(PageRankMapperZero.class);
	    job.setReducerClass(PageRankReducerZero.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);     
        job.waitForCompletion(true);
        long totalNodes = job.getCounters().findCounter(PageRankEnum.TOTAL_NODES).getValue();
        int round = 1;
        while (round < 6){
        	String inputFile = outputFile;
        	outputFile = args[1] + " pass " + round;
        	conf = new Configuration();
        	conf.setLong("TOTAL_NODES", totalNodes);
        	job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");

        	FileInputFormat.setInputPaths(job, new Path(inputFile));
     	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
     	    
     	    job.setMapperClass(PageRankMapper.class);
     	    job.setReducerClass(PageRankReducer.class);
     	    job.setOutputKeyClass(LongWritable.class);
     	    job.setOutputValueClass(Text.class);
             	
     	    job.setInputFormatClass(SequenceFileInputFormat.class);
     	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
             
            job.waitForCompletion(true);
            round++;
        	
        }
	    

	   
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	}

        
}
