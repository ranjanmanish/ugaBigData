import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KMeanController extends Configured implements Tool{
	public static enum KMean {
		CONVERGED
	}
	//private Configuration config;
	public int run(String[] args) throws Exception
	{
		//Configuration conf = getConf();
		Configuration distConfig = new Configuration();
		
		//JobConf jobdc = new JobConf();
		Path cache = new Path("/user/cloudera/cluster/inputCentroid/");
		FileSystem fs = cache.getFileSystem(distConfig);
		Path pathPattern = new Path(new Path("/user/cloudera/cluster/inputCentroid/"), "centroids10.small.txt");
		FileStatus [] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			System.out.println(status.getPath().toUri().toString());
			DistributedCache.addCacheFile(status.getPath().toUri(), distConfig);
		}
		int numReducers = distConfig.getInt("reducers",2 );
		
		Job job = Job.getInstance(distConfig,"conf1");
				
		job.setJarByClass(KMeanController.class);
		job.setNumReduceTasks(numReducers);
		job.setJobName("Kmean");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(KMeanMapper.class);
		//job.setCombinerClass(KMeanCombiner.class);
		job.setReducerClass(KMeanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		// this main function will call run method defined above.
		int res = ToolRunner.run(new Configuration(), new KMeanController(),args);
		System.exit(res);
	}
}