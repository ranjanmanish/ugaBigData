import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class NaiveClassifierMethods {
	private static final String DELIMETER = "\t";
	private static final String COMADELIMETER = ",";

	/**
	 * @author manish
	 * @param args
	 * creatintg this class to break the problem in methods first. Hoping to use these methods directly in finally Map Reduce paradigm
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		NaiveClassifierMethods nvObj = new NaiveClassifierMethods();
		Set<String> uniqTken = new HashSet<String>();
		Map<String, List<String>> mapOfLabelToWord = new HashMap<String, List<String>>();
		File f = new File("/home/manish/Desktop/MapReduce/NaiveData/RCV1.very_small_train.txt");
		// to establish File is there
		if(f.exists())
			System.out.println("Well Hello there , File is here");
		else
			System.out.println("Well You know what it means");

		//Now finding all the unique levels

		findUniqueLabelsList(f);

		// to find all the no of docs
		countNoOfDoc(f);

		//No of tokens for a given level( Unique Tokens)
		for(String str: uniqTken){
			mapOfLabelToWord.put(str, getWordsforToken(f,str));
		}
		
	}

	private static List<String> getWordsforToken(File f, String str) throws FileNotFoundException, IOException {
		try(BufferedReader br = new BufferedReader(new FileReader(f))) {
			for(String line; (line = br.readLine()) != null; ) {
				String[] wordArray = line.split(DELIMETER);
			}
		}
		return null;
	}

	private static void countNoOfDoc(File f) throws FileNotFoundException, IOException {
		int docCounter = 0;
		try(BufferedReader br = new BufferedReader(new FileReader(f))) {
			for(String line; (line = br.readLine()) != null; ) {
				docCounter++;
			}
		}
		System.out.println("Total no of Document is "+docCounter);
	}

	/**
	 * This method to find all the unique labels across given file has many document 
	 * @param f
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void findUniqueLabelsList(File f) throws FileNotFoundException, IOException {
		Set<String> uniqueLabels= new HashSet<String>();
		try(BufferedReader br = new BufferedReader(new FileReader(f))) {
			for(String line; (line = br.readLine()) != null; ) {
				//System.out.println(line);
				String[] lwordSep  = line.split(DELIMETER);
				System.out.println(lwordSep[0]);
				String[] labels = lwordSep[0].split(COMADELIMETER);
				for(String label:labels){
					uniqueLabels.add(label);
				}
			}
			System.out.println("All the labels are");
			for(String lbl:uniqueLabels){
				System.out.println(lbl);
			}
		}
	}

}
