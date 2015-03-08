import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @author Manish Ranjan
 * Post discussion with Dr Quinn
 * took reference from Dr Quinn's blog as well.
 *
 */
public class NaiveClassifyRed extends
Reducer<LongWritable, Text, LongWritable, IntWritable> {
	private long totalDocuments;
	private long uniqueLabels;
	private HashMap<String, Integer> docsWithLabel;
	@Override
	protected void setup(Context context) throws IOException {
		totalDocuments = context.getConfiguration().getLong(NaiveBayesController.TOTALNO_DOCS, 100);
		uniqueLabels = context.getConfiguration().getLong(NaiveBayesController.UNIQUE_LABELS, 100);
		docsWithLabel = new HashMap<String, Integer>();
		// Build a HashMap of the label data in the DistributedCache.
		Path [] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length < 1) {
			throw new IOException("DistributedCache returned an empty file set!");
		}
		// Read in from the DistributedCache.
		LocalFileSystem lfs = FileSystem.getLocal(context.getConfiguration());
		for (Path file : files) {
			FSDataInputStream input = lfs.open(file);
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String [] elems = line.split("\\s+");
				String label = elems[0];
				String [] counts = elems[1].split(":");
				docsWithLabel.put(label, new Integer(Integer.parseInt(counts[0])));
			}
			IOUtils.closeStream(in);
		}
	} 
	// this is what mapper sent  // So all the info with one doc ID is here We should be able to sum up on each label and 
	// come up with conclusion ?
 	//1. Doc Id: So we can do a sum of prob : as multiple mappers might be working on it
	// list of "<label:probability>
	// list of "<truelabel>
	// based on my trace this is what I *expect* to arrive at reducer 
	// ID:label1:prob1,label2:prob2,label3:prob3::<label1,Label2>
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws InterruptedException, IOException {
		// Lots of metadata.
		HashMap<String, Double> probabilities = new HashMap<String, Double>();
		ArrayList<String> trueLabels = null;
		for (Text value : values) {
			// Each value is a list of label probabilities for a single word.
			//
			String [] elements = value.toString().split("::");  // ID:label1:prob1,label2:prob2,label3:prob3 :: <label1,Label2>
			String [] labelProbabs = elements[0].split(",");  
			for (String labelProb : labelProbabs) {
				String [] pieces = labelProb.split(":");
				String label = pieces[0];
				double prob = Double.parseDouble(pieces[1]);
				probabilities.put(label, new Double(
						probabilities.containsKey(label) ? probabilities.get(label).doubleValue() + prob : prob));
			}
			// The list of true labels so I can verify and take a final call
			if (trueLabels == null) {
				String [] list = elements[1].split(":");
				trueLabels = new ArrayList<String>();
				for (String elem : list) {
					trueLabels.add(elem);	
				}
			}
		}
		// Now, loop through each label, adding in the prior for it and
		// determining what label is most likely.
		double bestProb = Double.NEGATIVE_INFINITY;  // Why does it have issue with 0.0 , looked at your code for this initialization 
		String bestLabel = null;
		for (String label : probabilities.keySet()) {
			
			double totalProb = Math.log(probabilities.get(label).doubleValue()) + Math.log((double)docsWithLabel.get(label).intValue()) - Math.log(totalDocuments); // Just wrote the formula from ppt in Java here 
			System.out.println("Total Prob"+totalProb);
			System.out.println("Total Prob"+bestProb);
			if (totalProb > bestProb) {
				bestLabel = label;
				bestProb = totalProb;
			}
		}
		
		context.write(key, new IntWritable(trueLabels.contains(bestLabel) ? 1 : 0));
	}
}