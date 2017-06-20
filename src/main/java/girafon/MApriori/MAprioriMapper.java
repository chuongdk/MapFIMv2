package girafon.MApriori;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MAprioriMapper
    extends Mapper<Object, Text, Text, Text>{
	
 
	 private Text word = new Text();

	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
		 String[] splits = value.toString().split("\\s+");
	
		 // splits into a list of integer
		 List<Integer> t = new ArrayList<Integer>();

		 for(int i=0; i<splits.length; i++) {
		    t.add(Integer.parseInt(splits[i]));
		 }
		// we sort transaction t:
		 Collections.sort(t);
		 
		 //System.out.println(t);
		 for (int i = 0; i < t.size(); i++) {
			 String exportKey = t.get(i).toString();
			 String exportValue = "1 " + (t.size() - i - 1);
			 //System.out.println(exportKey + " ------ " + exportValue);
			 context.write(new Text(exportKey), new Text(exportValue));
		 }
	   
	 }
}
