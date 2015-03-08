/**
 * CSCI6900
 * @author manish
 * 
 */
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveJoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws InterruptedException, IOException {
		String training = null;
		ArrayList<String> testList = new ArrayList<String>();
		System.out.println("******************************************************");
		for (Text value : values) {
			//System.out.println(value);
			String line = value.toString();
			if (line.toString().contains("-")) {
				// Line is from the wordMapper output
				training = line.toString();
			} else {
				// it is coming from the test document which has labels and the document text.
				testList.add(line.toString());
			}
		}
		if (testList.size() > 0) {
		
			if (training == null) {
				training = "";
			}
			
			// now all I am looking for is appending both the incoming docs with common word key 
			
			StringBuilder output = new StringBuilder(); // from training
			output.append(String.format("%s##",training));
			//output.append("&");
			// and now appending to same output from document
			//Now I am trying t separate the training and test part by introducing delimiters 
			
			for (String doc : testList) { // from testing
				//output.append(String.format("%s#",doc)); //working
				output.append(String.format("%s##",doc));
			}
			
			String out = output.toString();
			context.write(key, new Text(out.substring(0, out.length() - 2)));
		}
	}
}

// Counter Concept and naming Conventions borrowed from 
//http://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/
// I also read upon your blog posted by you when you were in cmu - on your approach , But the entire coding is done by me