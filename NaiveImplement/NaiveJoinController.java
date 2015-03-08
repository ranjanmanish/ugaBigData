import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


public class NaiveJoinController extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		Job job3 = Job.getInstance(conf,"joinConf");
		job3.setJarByClass(NaiveJoinController.class);
		
		job3.setJobName("JoinMapperReducer");
		
		MultipleInputs.addInputPath(job3, new Path("/tmp2/"), TextInputFormat.class,   // generated file from word mapper
				NaiveJoinMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class,  // input test file
				NaivejoinTestMapper.class);
		
		job3.setReducerClass(NaiveJoinReducer.class);
		
		//job3.setInputFormatClass(SequenceFileInputFormat);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		
		FileOutputFormat.setOutputPath(job3, new Path("/tmp3/"));
		
		job3.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception
	{
		// this main function will call run method defined above.
		int res = ToolRunner.run(new Configuration(), new NaiveJoinController(),args);
		System.exit(res);
	}

}
