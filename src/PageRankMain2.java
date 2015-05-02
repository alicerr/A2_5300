
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
public class PageRankMain2 {
	public static void main(String[] args) throws Exception {
	    Configuration conf;
	    Job job;
	    String outputFile = args[0] + " pass 0";
	    int totalNodes = 685230;
        int round = 1;
        while (round < 2){
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
            job.setJarByClass(PageRankMain.class);
     	    job.setInputFormatClass(SequenceFileInputFormat.class);
     	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
            
            job.waitForCompletion(true);
            double residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;

            System.out.println("Round: " + round + 
        			"\nResidual sum (across all nodes): " + residualSum + " avg: " + residualSum/totalNodes + "\n");
            
            round++;
        	
        }
	    

        
	}

        
}
