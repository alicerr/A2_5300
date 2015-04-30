import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TestPageRank {
	
	public TestPageRank() throws IOException, ClassNotFoundException, InterruptedException{
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "page rank " + "test" + " pass 0");
		    //job.setJarByClass(WordCount.class);
		 job.setJarByClass(TestPageRank.class);
		 job.setMapperClass(PageRankMapper.class);
		 job.setReducerClass(PageRankReducer.class);
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 Path in = new Path("edges_no_spaces_3.txt");
	     Path out = new Path("test" + " pass 0");
	     FileInputFormat.addInputPath(job, in);
	     FileSystem fs = FileSystem.get(conf);
	     if (fs.exists(out))
	            fs.delete(out, true);
		    //FileInputFormat.addInputPath(job, new Path(args[0]));
		    //FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    //System.exit(job.waitForCompletion(true) ? 0 : 1);
	     SequenceFileOutputFormat.setOutputPath(job, out);
	     job.setOutputFormatClass(SequenceFileOutputFormat.class);
	     job.setOutputKeyClass(IntWritable.class);
	     job.setOutputValueClass(Text.class);
	     
	     

	}
	public static void main(String[] args) throws Exception {
		
		new TestPageRank();
	}
	
}

