import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NaiveWordMapper extends Mapper <LongWritable, Text, Text, Text>{
	public static final String WHITESPACE = "\\s+";
	public static final String TABDELIMETER = "\t+";

	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
	    String[] words = value.toString().split("\\s+");
	    Vector<String> labels = NaiveController.getTokenizeLabels(words[0]);
	    Vector<String> text = NaiveController.getTokenizeDoc(words);
	 
	    for (String label : labels) {
	    	if(label.matches(".*CAT.*"))
	           for (String word : text) {
	            // (Y = y, W = w)
	            context.write(new Text(word), new Text(label));
	        }
	    }
	}
}
