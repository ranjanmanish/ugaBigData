import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NaiveJoinMapper extends Mapper <Text, Text, Text, Text>{
	
	/**
	 * this class just takes the input file which is created by NaiveWordmapper and passes it so reducer based join can happen
	 */
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		context.write(key,value);
	}
}
