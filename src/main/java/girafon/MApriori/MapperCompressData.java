package girafon.MApriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperCompressData 
    extends Mapper<Object, Text, Text, Text>{
    	
    // Hasp Map of Items
    HashMap<String, Integer> hashItems = new HashMap<String, Integer>();
    
    private int nItems = 0;
	private Configuration conf;
    
    // read all frequent item from the cache
    private void getL1(Context context) throws IOException {
    	// read L1 from output/1/part*
    	Path l1Path = new Path(conf.get("output") 
				+ System.getProperty("file.separator")  
				+ "1");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(l1Path);

    	// get caches files.
		for (int i = 0; i < 5; i++)
			System.out.println("SETUP ------ MAPPER ----------- GET FREQUENT ITEMS--------------");
		
		System.out.println("Reading L1");
		
		nItems = 0;
		
		for(int i = 0; i<status.length; i++){
			if(status[i].getPath().toString().contains("part-r-")) { 
				Path p = status[i].getPath();
				InputStreamReader ir = new InputStreamReader(fs.open(p));
				BufferedReader data = new BufferedReader(ir);
				while (data.ready()) {    		
					String line=data.readLine();
					if (line.matches("\\s*")) continue; // be friendly with empty lines
					String[] numberStrings = line.split("\t");
					String frequentItem =  numberStrings[0];
	    			if (!hashItems.containsKey(frequentItem)) {
	        				hashItems.put(frequentItem, nItems);
	        				nItems++;
	        			}
	        		}
	    		
	    	}  
		}

   }
    
    
    protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("Mapper getting Cache -- Reading Frequent Items ---------");
		conf = context.getConfiguration();
		getL1(context); 
		return;
	}

	 
	 	  
	 
	 // Build the Trie. Reading one transaction => Add to trie with support count
	 @Override
	 public void map(Object key, Text value, Context context
	 
	                 ) throws IOException, InterruptedException {
		// read line by line, with each transaction, verify if a candidate is all in this transaction, if yes, write candiate, one
		// value is a line
		 
		String line = value.toString();
		// convert transaction to List<Integer>
		String[] s = line.split("\\s+");
		List<String> t = new ArrayList<String>();
		// count how many frequent items appear in this transaction
		int count = 0;
		for(int i=0; i<s.length; i++) {
		   t.add(s[i]);
		   if (hashItems.containsKey(s[i]))
				count++;
		}
		if (count >= 2) {
			 Text word = new Text();
			 String newT = new String();
			 // we sort transaction t:
			 Collections.sort(t);
			 // convert transaction to List<Integer>
			 for (String x: t)
				 if (hashItems.containsKey(x)) {
					newT = newT + x.toString() + "\t";					
				 }
			 // now newT is the transaction with only frequent items, we will output it
			 word.set(newT);
			 context.write(word, new Text("1\t1"));  // 1 support, 1 for data (to avoid error)
		}
	 }
}

