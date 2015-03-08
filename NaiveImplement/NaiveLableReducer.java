/**
 * CSCI6900
 * @author manish
 * 
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveLableReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws InterruptedException, IOException {
    	
    	// Every time a reducer Starts -> New unique label
    	context.getCounter(NaiveController.NAIVE_COUNTER.UNIQUE_LABELS).increment(1);
        // The data here should come in format : A : 6,5, B:6, C:4,2 ...< say A, B , C .. represent labels and nos represent the no of words associated with them
    	// if I reduce this I may get the total no of words with each label 
    	// if I know the total no of words doc label Y has, I would be able to compute the prob of finding a word under a given label?
    	long ndocWithThisLabel = 0; // no of doc with a label
    	long wordsforThisLabel = 0;
    	for(IntWritable value: values){
    		// increment the document count 
    		context.getCounter(NaiveController.NAIVE_COUNTER.TOTALNO_DOCS).increment(1);
    		ndocWithThisLabel++; // list will have all the values like <A:4,5,6> - I Can conclude that there are 3 documents with label A
    		// total no of words under this label 
    		wordsforThisLabel+=value.get();
    	}
    	context.write(key, new Text(String.format("%s:%s", ndocWithThisLabel, wordsforThisLabel)));
    }
    
}

// Counter Concept and naming Conventions borrowed from 
//http://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/
// I also read upon your blog posted by you when you were in cmu - on your approach , But the entire coding is done by me 

