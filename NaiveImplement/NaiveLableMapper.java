import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NaiveLableMapper extends Mapper <LongWritable, Text, Text, IntWritable>{
	public static final String WHITESPACE = "\\s+";
	public static final String TABDELIMETER = "\t+";
	
	
    public void map(LongWritable key, Text value, Context context) 
            throws InterruptedException, IOException {
    	List<String> labelsArray = Arrays.asList("MCAT", "CCAT", "ECAT", "GCAT");
        String[] words = value.toString().split(WHITESPACE);
        List<String> labels = NaiveController.getTokenizeLabels(words[0]);
        List<String> word = NaiveController.getTokenizeDoc(words);  
        
        for (String label : labels) {
        	if(labelsArray.contains(label))
            context.write(new Text(label), new IntWritable(word.size()));
        }
    }

}
