import java.util.HashMap;



public class ProcessingJoinTest {
	public static void main(String[] args) {
		String value = "was	CCAT-32,ECAT-15,MCAT-29,GCAT-34##7481,ECAT,GCAT##10762,CCAT##7481,ECAT,GCAT##7481,ECAT,GCAT";
		String [] elements = value.toString().split("##");
		String model = elements[0];
		// Create a HashMap for the model counts.
		HashMap<String, Integer> modelCounts = null;
		if (model.length() > 0) {
			String [] labelCounts = model.split("\t");
			modelCounts = new HashMap<String, Integer>();
			for(int i = 1; i < labelCounts.length; ++i){
				String []array = labelCounts[i].split(",");
				for(int j = 0 ;j < array.length; j++){
					String [] elems = array[j].split("-");
					modelCounts.put(elems[0], new Integer(Integer.parseInt(elems[1])));
				}
			}
			HashMap<Long, Integer> labelSumup = new HashMap<Long, Integer>();

			for(int i = 1 ; i< elements.length ;++i){
				String[] val = elements[i].split(",");
				Long docId = new Long(Long.parseLong(val[0]));  // basically just keep adding if multiple occurences are found
				labelSumup.put(docId, new Integer(labelSumup.containsKey(docId) ? labelSumup.get(docId).intValue() + 1 : 1)); 
			}
			
			
			for (String labelCount : labelCounts) { 
				String [] elems = labelCounts[1].split("-");
				modelCounts.put(elems[0], new Integer(Integer.parseInt(elems[1])));
			}
		}
		// How many times did this word show up in each document?
		HashMap<Long, Integer> multipliers = new HashMap<Long, Integer>();
		HashMap<Long, String> trueLabels = new HashMap<Long, String>();
		for (int i = 1; i < elements.length; ++i) {
			String [] elems = elements[i].split(",");
			Long docId = new Long(Long.parseLong(elems[0]));
			multipliers.put(docId, new Integer(
					multipliers.containsKey(docId) ? multipliers.get(docId).intValue() + 1 : 1));
			if (!trueLabels.containsKey(docId)) {
				// Add the list of true labels for this document.
				// ASSUMPTION: The same document ID will have the same true labels.
				StringBuilder list = new StringBuilder();
				for (int j = 1; j < elems.length; ++j) {
					list.append(String.format("%s:", elems[j]));
				}
				String outval = list.toString();
				trueLabels.put(docId, outval.substring(0, outval.length() - 1));
			}
		}
	}
}
