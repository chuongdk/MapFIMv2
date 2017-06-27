package girafon.MApriori;

import java.io.IOException;



import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.io.InputStreamReader;
import java.io.BufferedReader;


// save transaction to memory
// mine at cleanUp
// read candidates in multi iterations

public class MapperGlobalPhase 
    extends Mapper<Object, Text, Text, Text>{
	
	private List<String> data = new ArrayList<String>();  
	private Configuration conf;
    
	// candidate Trie that we will mine
	private Trie candidateTrie;
	
	private int maxTrieSize;
	
	private Path candidatePath;
	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		Integer prevIteration  = conf.getInt("iteration", 0) - 1;
		maxTrieSize = conf.getInt("maxTrieSize", 0);
		candidatePath = new Path(conf.get("output") 
					+ System.getProperty("file.separator")  
					+ prevIteration.toString() 
					+ System.getProperty("file.separator")
					+ "candidate" );	
	}

	 

	  
	 
	 // save transactions to memory
	 @Override
	 public void map(Object key, Text value, Context context
	 
	                 ) throws IOException, InterruptedException {
		// each line contains a transaction with the occurences.  1 2 3    7 => transaction (1,2,3) apears 7 times
		 data.add(value.toString());
		 
	 }

	 
	 private void mining(Context context) throws IOException, InterruptedException {
			// We have candidateTrie + data. Now we mine
			for(String line:data){
				String[] s = line.split("\\s+");
				List<Integer> t = new ArrayList<Integer>();
				for(int i=0; i<s.length; i++)
				   t.add(Integer.parseInt(s[i]));
				// update support in Trie with transaction t
				candidateTrie.updateSupport(t);
				
			}
 
			// Mining is done. Now export to Reducers
			 List<Integer> itemset = new ArrayList<Integer>();
			 outToReducer(context, candidateTrie, itemset);		 
	 }
	 
	 
	 // Get candidates and mine with Data
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 // Getting Candidates
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(candidatePath);
			candidateTrie = new Trie(-1);
			
			int totalNode = 0;
			
			for(int i = 0; i<status.length; i++){ 
				InputStreamReader ir = new InputStreamReader(fs.open(status[i].getPath()));
				BufferedReader data = new BufferedReader(ir);
		    	while (data.ready()) {    		
		    		String line=data.readLine();
		    		if (line.matches("\\s*")) continue; // be friendly with empty lines
		    		// creat new prefix tempPrefix
		    		List<Integer> tempPrefix = new ArrayList<Integer>();
		    		String[] numberStrings = line.split("\\s+");
		    		for (int x = 0; x < numberStrings.length; x++){   
		    			tempPrefix.add(Integer.parseInt(numberStrings[x]));
		    		}    		
		    		// add p to the list of prefix
		    		candidateTrie.addToTrie(tempPrefix);

		    		totalNode += tempPrefix.size();
			    	if (totalNode >= maxTrieSize) {
			    		 System.out.println("\n\n\n\nMining with #node = " + totalNode);
			    		mining(context);
			    		totalNode = 0;
			    		candidateTrie = new Trie(-1);
			    	}

		    	}  
		    	
		    	
			}
			
	    	if (totalNode > 0) {
 	    		System.out.println("\n\n\n\nFinal Mining with #node = " + totalNode);
	    		mining(context);
	    	}
 
			  		
	 
	 }
	 
	 public void outToReducer(Context context, Trie trie, List<Integer> currentPrefix) throws IOException, InterruptedException {
	      for (Integer x : trie.children.keySet()) {
	    	Trie nextTrie = trie.children.get(x);
	    	List<Integer> nextPrefix =  new ArrayList<>();
	    	
	    	for (Integer z : currentPrefix)
	    		nextPrefix.add(z);
	    	
	    	nextPrefix.add(x);
	    	
	    	// a node is a leaf if its children = nullhs
	    	if (nextTrie.children == null) {
	    		Text key = new Text();
	    		key.set(itemsetToString(nextPrefix));
//	    		IntWritable value =  new IntWritable(nextTrie.support);
	    		String exportValue = nextTrie.support + " " + nextTrie.data;
	    		//System.out.println(key.toString() + ", value=" + exportValue);
	    		context.write(key, new Text(exportValue) );
	    	}
	    	else
	    		outToReducer(context, nextTrie, nextPrefix);
	      }
	 }	 

	 public static String itemsetToString(List<Integer> x) {
		 String a = new String();
		 a = x.get(0) + "";
		 for (int i = 1; i < x.size(); i++)
			 a = a + "\t" + x.get(i);
		 return a;
	 }

}

