import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class NaiveClassifyMap extends Mapper <Text, Text, LongWritable, Text>{

	/**
	 * @author manish ranjan
	 * this class just takes the input file which is created by NaiveWordmapper and passes it so reducer based join can happen
	 * Discussed initially on classifier on paper with Beta and Alekhya : Followed by help from Prof Quinn himself
	 */
	private Long VOCABSIZE;
	private HashMap<String, Integer> wordsLabel;

	protected void setup(Context context) throws IOException{
		Counter counter  = context.getCounter(NaiveBayesController.NAIVE_COUNTER.VOCAB_SIZE);
		VOCABSIZE= counter.getValue();
		wordsLabel = new HashMap<String,Integer>();

		Path [] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null) {
			throw new IOException("DistributedCache returned an empty file set!");
		}
		// Reading the label file via Distributed Cache
		LocalFileSystem lfs = FileSystem.getLocal(context.getConfiguration());
		for (Path file : files) {
			FSDataInputStream input = lfs.open(file);
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String [] elems = line.split("\\s+"); // Separating based on whitespace
				String label = elems[0];
				String [] counts = elems[1].split(":");  //ECAT 12:3340 we are interested in 3340 number here
				wordsLabel.put(label, new Integer(Integer.parseInt(counts[1])));   // I will have a map like ECAT:3340,CCAT:7890 etc
			}
			IOUtils.closeStream(in);
		}


		//VOCABSIZE= context.getConfiguration().getLong(NaiveBayesController.NAIVE_COUNTER.VOCAB_SIZE, 100);

		// Joined o/p is what I am gonna process here 
		// very	CCAT-4,ECAT-1,MCAT-2,GCAT-2##10762,CCAT##16802,CCAT
	}

	public void map(Text key, Text value, Context context)
			throws InterruptedException, IOException {
		String [] values = value.toString().split("##");
		String wordMapperOutput  = values[0];

		Map<String,Integer> wordCount = new HashMap<String,Integer>();
		if(wordMapperOutput.length()> 0){   // threw Number Format exception - probably my joined o/p is not as well formatted as I thought
			String [] label = wordMapperOutput.split("\t");
			/*for(int i = 1; i < labelCounts.length; ++i){
			String []array = labelCounts[i].split(",");
			for(int j = 0 ;j < array.length; j++){
				String [] elems = array[j].split("-");
				modelCounts.put(elems[0], new Integer(Integer.parseInt(elems[1])));
			}
		}*/
			for(int i = 1; i < label.length; ++i){
				String[] array = label[i].split(",");
				for(int j = 0 ; j < array.length; j++){
					String []labelCounts = array[j].split("-");
					if(labelCounts.length>1)
						wordCount.put(labelCounts[0], Integer.parseInt(labelCounts[1]));   // this will come handy when I need C(x=w1,Y=LABEL1)
				}
			}
		}
		// now the no of times this word was found in each document

		HashMap<Long, Integer> labelSumup = new HashMap<Long, Integer>();
		HashMap<Long, String> CorrectLabelList = new HashMap<Long, String>();

		// this is what I am trying to parse 
		// waiting	MCAT-2|0,CCAT,MCAT|0,CCAT,MCAT|0,CCAT,MCAT .....

		// iterate through the array which has all the required infor split on | delimeter
		// at i = 0 has the GCAT info , after the first split is where the doc id is placed

		for(int i = 1 ; i< values.length ;++i){
			String[] val = values[i].split(",");
			Long docId = new Long(Long.parseLong(val[0]));  // basically just keep adding if multiple occurences are found
			labelSumup.put(docId, new Integer(labelSumup.containsKey(docId) ? labelSumup.get(docId).intValue() + 1 : 1)); // to keep a tab on , if multiple occurence of same label was found
			// this data structure should look like {(12345,ECAT),()}
			// now I want to create the total true list of labels fro a given doc Id 

			if (!CorrectLabelList.containsKey(docId)) {
				// Add the list of true labels for this document.

				StringBuilder list = new StringBuilder();
				for (int j = 1; j < val.length; ++j) {
					list.append(String.format("%s:", val[j])); //{(docID:label1,label2}
				}

				String outval = list.toString();
				CorrectLabelList.put(docId, outval.substring(0, outval.length() - 1));
			}
		}

		for (Long docId : CorrectLabelList.keySet()) {
			StringBuilder probs = new StringBuilder();
			for (String labls : wordsLabel.keySet()) { // Computing the probability for each label 
				int wordLabelCount = wordsLabel.get(labls).intValue();
				int count = 0;
				if (wordCount != null && wordCount.containsKey(labls)) {
					count = wordCount.get(labls).intValue();  // count for that word under that label
				}

				int multiplier = labelSumup.get(docId);
				if(wordLabelCount!=0){   // Just so that it does not divide it by 0 - returns in infinity 
					double wordProb = (double)multiplier-Math.log(count) - Math.log(wordLabelCount + VOCABSIZE) ;         // Formula Implementation :  
					probs.append(String.format("%s:%s,", labls, wordProb));
				}
			}
			// So to be able to now get work done in reducer we need 3 info
			//1. Doc Id: So we can do a sum of prob : as multiple mappers might be working on it
			// list of "<label:probability>
			// list of "<truelabel>:So we can finally justify our output against this and come up with a final decision
			String output = probs.toString();
			context.write(new LongWritable(docId),
					new Text(String.format("%s::%s", output.substring(0, output.length() - 1), CorrectLabelList.get(docId))));
		}
	}
}
