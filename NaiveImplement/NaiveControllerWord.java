import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NaiveControllerWord extends Configured implements Tool{
	public static enum NAIVE_COUNTER {
		UNIQUE_LABELS,
		VOCAB_SIZE,
		TOTALNO_DOCS
	}
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());
		job.setJarByClass(NaiveControllerWord.class);
		job.setJobName("Max temperature");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(NaiveWordMapper.class);
		job.setReducerClass(NaiveWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		// this main function will call run method defined above.
		int res = ToolRunner.run(new Configuration(), new NaiveControllerWord(),args);
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