import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author Manish Ranjan
 * This is Controller class for all the Map Reduce Jobs
 *
 */


public class NaiveBayesController extends Configured implements Tool{
	static enum NAIVE_COUNTER {
		UNIQUE_LABELS,
		VOCAB_SIZE,
		TOTALNO_DOCS
	}

	public static final String  UNIQUE_LABELS = "UL";
	public static final String  VOCAB_SIZE = "VS";
	public static final String  TOTALNO_DOCS = "TND";
	/**
	 * to many manual deletions , have been handling via shell so far , good to write a method for this
	 * @throws IOException 
	 */

	public static void deleteFile(Configuration cnf , Path path) throws IOException{
		FileSystem fs = path.getFileSystem(cnf);
		if(fs.exists(path)){
			fs.delete(path,true); // delete with single arg shows deprecated !
		}
	}


	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();
		Configuration classifyConfig = new Configuration();
		int numReducers = conf.getInt("reducers",2 );

		// looking back easiest part !
		
		Job labelJob = Job.getInstance(conf,"conf1");
		
		
		labelJob.setJarByClass(NaiveBayesController.class);
		labelJob.setNumReduceTasks(numReducers);
		labelJob.setJobName("Naive Label");
		FileInputFormat.addInputPath(labelJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(labelJob, new Path("/tmp11_2l/"));
		labelJob.setMapperClass(NaiveLableMapper.class);
		labelJob.setReducerClass(NaiveLableReducer.class);
		labelJob.setOutputKeyClass(Text.class);
		labelJob.setOutputValueClass(IntWritable.class);
		labelJob.waitForCompletion(true);



		classifyConfig.setLong(NaiveBayesController.UNIQUE_LABELS,
				labelJob.getCounters().findCounter(NaiveBayesController.NAIVE_COUNTER.UNIQUE_LABELS).getValue());

		classifyConfig.setLong(NaiveBayesController.TOTALNO_DOCS,
				labelJob.getCounters().findCounter(NaiveBayesController.NAIVE_COUNTER.TOTALNO_DOCS).getValue());


		// Now for Naive Word Calculation 

		//Job job2 = new Job(getConf());
		Job job2 =Job.getInstance(conf, "conf");
		job2.setJarByClass(NaiveBayesController.class);
		job2.setNumReduceTasks(numReducers);
		job2.setJobName("Max temperature");
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path("/tmp21_2l/"));
		job2.setMapperClass(NaiveWordMapper.class);
		job2.setReducerClass(NaiveWordReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);

		classifyConfig.setLong(NaiveBayesController.VOCAB_SIZE,
				labelJob.getCounters().findCounter(NaiveBayesController.NAIVE_COUNTER.VOCAB_SIZE).getValue());

		// Now comes the reducer side join part
		// Discussed on board with CS Phd Student Muthu  to get hold on this one
		
		Job job3 = Job.getInstance(conf,"joinConf");
		job3.setJarByClass(NaiveJoinController.class);
		job3.setNumReduceTasks(numReducers);
		job3.setJobName("JoinMapperReducer");

		MultipleInputs.addInputPath(job3, new Path("/tmp21_2l/"), KeyValueTextInputFormat.class,
				NaiveJoinMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[1]), TextInputFormat.class,
				NaivejoinTestMapper.class);

		job3.setReducerClass(NaiveJoinReducer.class);

		job3.setOutputFormatClass(TextOutputFormat.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job3, new Path("/tmp31_2l/"));

		job3.waitForCompletion(true);

		//Now pasting the classifier part 
		// Borrowed idea from Dr Quinn on implementation - as I was getting clueless - My First ML Algo Implmentation :) 

		Path cache = new Path("/temp11_2l/");
		FileSystem fs = cache.getFileSystem(classifyConfig);
		Path pathPattern = new Path(new Path("/tmp11_2l/"), "part-r-[0-9]*");
		FileStatus [] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), classifyConfig);
		}

		Job job4 = Job.getInstance(classifyConfig,"classifyConf");
		job4.setJarByClass(NaiveBayesController.class);
		job4.setNumReduceTasks(numReducers);

		job4.setJobName("ClassifierMapperReducer");


		FileInputFormat.addInputPath(job4, new Path("/tmp31_2l/"));

		job4.setMapperClass(NaiveClassifyMap.class);
		job4.setReducerClass(NaiveClassifyRed.class);

		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		job4.setMapOutputKeyClass(LongWritable.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setOutputKeyClass(LongWritable.class);
		job4.setOutputValueClass(IntWritable.class);

		FileOutputFormat.setOutputPath(job4, new Path(args[2]));

		job4.waitForCompletion(true);


		// classifying right and read via read operation
		int correct = 0;
		int total = 0;
		pathPattern = new Path(args[2], "part-r-[0-9]*");
		FileStatus [] results = fs.globStatus(pathPattern);
		for (FileStatus result : results) {
			FSDataInputStream input = fs.open(result.getPath());
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String [] pieces = line.split("\t");
				correct += (Integer.parseInt(pieces[1]) == 1 ? 1 : 0);
				total++;
			}
			IOUtils.closeStream(in);
		}
		System.out.println(String.format("%s/%s, accuracy %.2f", correct, total, ((double)correct / (double)total) * 100.0));

		return 0;

	}

	public static void main(String[] args) throws Exception
	{
		// this main function will call run method defined above.
		int res = ToolRunner.run(new Configuration(), new NaiveBayesController(),args);
		System.exit(res);
	}

	public static Vector<String> getTokenizeLabels(String string) {
		String[] labelArray = string.split(",");
		Vector<String> labelList = new Vector<String>();
		for(String label:labelArray)labelList.add(label);
		return labelList;
	}

	/**
	 * copied directly from the assignment given method 
	 * @param words
	 * @return
	 */
	public static Vector<String> getTokenizeDoc(String[] words) {
		// 0th position is where all the labels are so we need to look from 1
		// create a arrayList to keep all the words
		Vector<String> wordList = new Vector<String>();
		for(int i = 1; i< words.length; i++){
			words[i] = words[i].replaceAll("\\W","");
			if(words[i].length() > 0)wordList.add(words[i]);
		}
		return wordList;
	}
}

//refrences:
//http://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/
//https://spectrallyclustered.wordpress.com/2013/02/20/naive-bayes-on-hadoop/
//https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/lib/input/MultipleInputs.html
