/**
 * CSCI6900
 * @author manish
 * 
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeanReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws InterruptedException, IOException {
		List<String>  list = new ArrayList<String>();
		int counter = 0;
		int anotherCounter = 0 ; 
		String justOneVector= "";
		List<String> listVal= new ArrayList<>();
		List<String> addedList = new ArrayList<>();
		for(Text text:values){
			counter++;
			listVal.clear(); // list should be cleaned up
			if(text.toString().length()> 0){
				anotherCounter++;
				String[] idandArray = text.toString().split(":");
				String[] vectorArray = idandArray[1].split(",");
				for(String str: vectorArray){
					listVal.add(str);
				}
				if(anotherCounter==1)
					addedList = listVal;
				else{
					addedList = KMeanCombiner.add(addedList,listVal);
				}
			}
		}
		if(counter > 1)  // either counter being 0 -problem or even 1 will have no impact on avg centrod hence avoid calling avg
			addedList = findAvg (addedList,counter);
		//context.write(key, new Text(String.format("%s:%s", counter+"", addedList.toString())));*/
		context.write(key, new Text(String.format("%s",addedList.toString().substring(1, addedList.toString().length()-1))));
	}
	/**
	 * Find average of the list based on the double list value and the counter value
	 * @param addedList
	 * @param counter
	 * @return
	 */
	private List<String> findAvg(List<String> addedList, int counter) {
		List<String> avgVector = new ArrayList<String>();
		for(String str: addedList){
			Double temp = Double.parseDouble(str)/counter;
			avgVector.add(temp.toString());
		}
		return avgVector;
	}

}

// Counter Concept and naming Conventions borrowed from 
//http://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/ 

