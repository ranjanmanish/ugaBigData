/**
 * CSCI6900
 * @author manish
 * 
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveWordReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
		    throws InterruptedException, IOException {
		 
		    // Update the counter to indicate the size of the vocabulary.
		    context.getCounter(NaiveControllerWord.NAIVE_COUNTER.VOCAB_SIZE).increment(1);
		 
		    // Loop through the labels.
		    Map<String, Integer> counts = new HashMap<String, Integer>();
		    for (Text label : values) {
		        String labelKey = label.toString();
		        counts.put(labelKey,
		                new Integer(counts.containsKey(labelKey) ? counts.get(labelKey).intValue() + 1 : 1));
		    }
		    StringBuilder outKey = new StringBuilder();
		    for (String label : counts.keySet()) {
		        outKey.append(String.format("%s-%s", label, counts.get(label).intValue())+",");
		    }
		 
		    // Write out the Map associated with the word.
		    context.write(key, new Text(outKey.toString().substring(0,outKey.length()-1)));
		}
}

//References for coding this entire exercise
// Counter Concept and naming Conventions borrowed from 
//http://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/
// I also read upon your blog posted by you when you were in cmu - on your approach , But the entire coding is done by me 

