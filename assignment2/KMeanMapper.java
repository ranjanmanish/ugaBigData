import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KMeanMapper extends Mapper <LongWritable, Text, Text, Text>{
	private Map<String, List<Double>> centroidVect;
	private Map<String , String> dummyMap = new HashMap<String,String>();
	private Configuration conf;
	private BufferedReader bufReader;
	private List<Double> cenroidVect;
	public static final String DELIMETER = "\\s+";
	protected void setup(Context context) throws IOException{
		centroidVect = new HashMap<>();
		conf = context.getConfiguration();
		Path [] files = context.getLocalCacheFiles();
		if (files == null) {
			throw new IOException("DistributedCache returned an empty file set!");
		}
		String fileName = files[0].toString();
		bufReader = new BufferedReader(new FileReader(fileName));
		String line= null;
		int counter = 0 ;
		String[] tempSplitter = null;
		while ((line = bufReader.readLine()) != null) {
			tempSplitter = line.split(DELIMETER);
			dummyMap.put(tempSplitter[0], tempSplitter[1]);
		}
		//centroidVect = Arrays.asList(tempSplitter[1]);
		IOUtils.closeStream(bufReader);
	}

	public void map(LongWritable key, Text value, Context context) 
			throws InterruptedException, IOException {

		String[] gist = value.toString().split(DELIMETER); // all the image variables
		Double findDistance = 0.0;
		int clusterId = Integer.MIN_VALUE;
		double distance = Double.MAX_VALUE;
		Map<String, Double> centroidAndDist = new HashMap<String,Double>(); // this map to store the distance of from each centroid 
		/*
		 * This for loop to get the shortest distant image centroid from the image vector in mapper right now
		 */
		for(Map.Entry<String, String> entry : dummyMap.entrySet()){
			findDistance = findEuclidianDistance(entry.getValue(),gist[1]);
			double sumOfSquare = 0.0;
			String centroid = entry.getValue();
			String image = gist[1];
			String[] centroidVect  = centroid.split(",");
			String[] imageVector = image.split(",");
			for(int i = 0 ; i < imageVector.length; ++i){
				double diff = Double.parseDouble(centroidVect[i]) - Double.parseDouble(imageVector[i]);
				sumOfSquare+= diff*diff;
			}
			sumOfSquare = Math.sqrt(sumOfSquare);
			if(sumOfSquare < distance){    // update only if distance calculated now is smaler than the exhisting distnace of image from centroid
				distance = sumOfSquare;
				clusterId = Integer.parseInt(entry.getKey());
			}		
		}
		context.write(new Text(""+clusterId), new Text(String.format("%s:%s", gist[0],gist[1])));
	}
/**
 * 
 * @param centroid
 * @param image
 * @return
 */
	private Double findEuclidianDistance(String centroid, String image) {
		double sumOfSquare = 0.0;
		String[] centroidVect  = centroid.split(",");
		String[] imageVector = image.split(",");
		for(int i = 0 ; i < imageVector.length; ++i){
			double diff = Double.parseDouble(centroidVect[i]) - Double.parseDouble(imageVector[i]);
			sumOfSquare+= diff*diff;
		}
		return null;
	}

}
