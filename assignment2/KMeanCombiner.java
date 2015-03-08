/**
 * CSCI6900
 * @author manish
 * this will create intermediate summed up vector so computing average at reducer is easy plus data flow can be reduced as well
 * the reason why to think in terms of combiners
 * 
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeanCombiner extends Reducer<Text, Text, Text, Text> {
	// <key value> <c1: <list{(no1, no2 , ... no384),(no1,no2,no3...no384),(no1,no2,....no384)}>
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws InterruptedException, IOException {
		int counter = 0;
		//List<Double> intermediateAvg = new ArrayList<>();
		// need to compute the sume here of all the incoming vectors so I can later spit average of them as o/p 
		// always remember- combiner may or may not run!! //
		// so the same logic should be handled in Reducer as well for sure so combiner is subset of reducer
		// planning to use this counter variable so I know how many values came say 3 lists for c1(centroid 1)
		String justOneVector= "";
		for(Text text:values){
			//			if(counter ==0)
			//				justOneVector = text.toString();
			counter++;
		}

		List<String> listVal = new ArrayList<String>();
		List<String> prevList = new ArrayList<String>();
		// if there is just one value corresponding to centroid key - then no point in going ahead with calculation
		// just write and step out 
		if(counter==1)
			context.write(key, new Text(String.format("%s:%s", counter+"",justOneVector)));

		for(Text text: values){
			if(text.toString().length()> 0){
				String[] idandArray = text.toString().split(":");
				String[] vectorArray = idandArray[1].split(",");
				for(String str: vectorArray){
					listVal.add(str);  // constructing the  comma separated array 
				}
				prevList = KMeanCombiner.add(prevList,listVal);
			}
		}
		context.write(key, new Text(String.format("%s:%s", counter+"", prevList.toString())));
		//context.write(key, new Text(String.format("%s:%s", counter+"", listVal.toString())));
	}
	/**
	 * Trying to add up  two lists element by element
	 * as I have handled the case where no data or single data comes as list
	 * hopefully this code can work smoothly 
	 * @param prevList
	 * @param listVal
	 * @return
	 */  // first no is very smal it should be 
	static List<String> add(List<String> prevList, List<String> listVal) {
		List<String> arrayList = new ArrayList<String>();
		Double temp=0.0;
		for(int i = 0 ; i < listVal.size()-1;){   /// Shit!!! how can you manish-an hour on this bug :(
			/*if(i==0){
				temp = temp + Double.parseDouble(listVal.get(i));
			}
			else{
				temp = Double.parseDouble(prevList.get(i)) + Double.parseDouble(listVal.get(i));
			}*/
			temp = Double.parseDouble(prevList.get(i)) + Double.parseDouble(listVal.get(++i));
			arrayList.add(temp.toString());
		}
		return arrayList;
	}
}



