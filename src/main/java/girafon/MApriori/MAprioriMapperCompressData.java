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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MAprioriMapperCompressData 
    extends Mapper<Object, Text, Text, Text>{
    	
    // Hasp Map of Items
    HashMap<Integer, Integer> hashItems = new HashMap<Integer, Integer>();
    
    private int nItems = 0;
     
    
    // read all frequent item from the cache
    private void getCache(Context context) throws IOException {
		 // Read file from distributed caches - each line is a item/itemset with its frequent

    	// get caches files.
		for (int i = 0; i < 10; i++)
			System.out.println("SETUP ------ MAPPER ----------- GET FREQUENT ITEMS--------------");
		
		
		// URI to locate cachefile, ex URI a = new URI("http://www.foo.com");
		List<URI> uris = Arrays.asList(context.getCacheFiles());
		
		System.out.println("Reading cached files");
		// read cache files which contains candidates?
		
		nItems = 0;
		
		for (URI uri : uris) {
			Path p = new Path(uri);
			System.out.println("Loading " + uri.toString());
			FileSystem fs = FileSystem.get(context.getConfiguration());
			InputStreamReader ir = new InputStreamReader(fs.open(p));
			BufferedReader data = new BufferedReader(ir);
	    	while (data.ready()) {    		
	    		String line=data.readLine();
	    		if (line.matches("\\s*")) continue; // be friendly with empty lines
	    		String[] numberStrings = line.split("\t");
	    		
 
	    		int frequentItem = Integer.parseInt(numberStrings[0]);
       			if (!hashItems.containsKey(frequentItem)) {
	        				hashItems.put(frequentItem, nItems);
	        				nItems++;
	        			}
	        		}
	    		
	    	}  

   }
    
    
    protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("Mapper getting Cache -- Reading Frequent Items ---------");
		getCache(context); 
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
		List<Integer> t = new ArrayList<Integer>();
		
		
		// count how many frequent items appear in this transaction
		int count = 0;
		for(int i=0; i<s.length; i++) {
		   t.add(Integer.parseInt(s[i]));
		   if (hashItems.containsKey(Integer.parseInt(s[i])))
				count++;
		}
		

		if (count >= 2) {
			 Text word = new Text();
			 String newT = new String();

			 // we sort transaction t:
			 Collections.sort(t);
			 
			 // convert transaction to List<Integer>
			 for (Integer x: t)
				 if (hashItems.containsKey(x)) {
					newT = newT + x.toString() + " ";					
				 }
			 // now newT is the transaction with only frequent items, we will output it
			 
			 
			 word.set(newT);

			 context.write(word, new Text("1 1"));  // 1 support, 1 for data (to avoid error)
			
		}
		 
		
		
			

		

	 }
	 
	 
	}

