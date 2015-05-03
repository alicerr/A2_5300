
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
     	    
     	    job.setJarByClass(PageRankMain.class);
     	    job.setMapperClass(PageRankMapper.class);
     	    job.setReducerClass(PageRankReducer.class);
     	    job.setOutputKeyClass(LongWritable.class);
     	    job.setOutputValueClass(Text.class);
            job.setJarByClass(PageRankMain.class);
            
            job.waitForCompletion(true);
            double residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;

            System.out.println("Round: " + round + 
        			"\nResidual sum (across all nodes): " + residualSum + " avg: " + residualSum/totalNodes + "\n");
            
            round++;
        	
        }
	    

	   
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	}

        
}
