/**
 * @author manish ranjan
 */
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NaivejoinTestMapper extends Mapper <LongWritable, Text, Text, Text>{
	public static final String WHITESPACE = "\\s+";
	public static final String TABDELIMETER = "\t+";

	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String[] words = value.toString().split(WHITESPACE);
		Vector<String> labels = NaiveController.getTokenizeLabels(words[0]);
		Vector<String> text = NaiveController.getTokenizeDoc(words);

		// now I need to build all the labels as one string as comma separated :so Finally this mapper should 
		// spit words as keys and all the labels 
		// This code will be practically similar to NaiveWordMapper as this class will also do almost same work 

		StringBuilder labelString = new StringBuilder();
		for (String label : labels) {
			if(label.matches(".*CAT.*")){
				labelString.append(label);
				labelString.append(",");
			}
		}
		
		String output = labelString.toString();
		// Output each word and its list of labels.
		for (String word : text) {
			//String result = key.get()+"::"+labelString;
			if(output.length()>2){
			context.write(new Text(word),
					new Text(String.format("%s,%s", key.get(), output.substring(0, output.length() - 1))));
			}
		}

	}
}
