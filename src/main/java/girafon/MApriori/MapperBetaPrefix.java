package girafon.MApriori;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.io.InputStreamReader;
import java.io.BufferedReader;


/* Read betaPrefix in the HDFS (output/betaPrefix
 * for each transaction, output to many reducers
 * key = [1 beta Prefix]
 * value = transaction after deleting the prefix
 * 
 */
public class MapperBetaPrefix
    extends Mapper<Object, Text, Text, Text>{
	
	
	
	// list of all prefix in the distributed cache 
    private List<List<String>> prefix = new  ArrayList<List<String>>();

    // Hasp Map of Items
    HashMap<String, Integer> hashItems = new HashMap<String, Integer>();

    private Trie betaPrefixTrie;
    
    private int nItems = 0;
    
    
    // Build Hash Map of Items
    private void buildHashItems() {
    	nItems = 0;
    	for (List<String> x : prefix) {
    		for (String i : x) {
    			if (!hashItems.containsKey(i)) {
    				hashItems.put(i, nItems);
    				nItems++;
    			}
    		}
    	}  	
    }
    
    
    private void getCache(Context context) throws IOException {
		 // Read file from distributed caches - each line is a item/itemset with its frequent
		Configuration config = context.getConfiguration();		
		// get caches files
		int k = config.getInt("iteration", 1);
		assert (k > 1);
		for (int i = 0; i < 3; i++)
			System.out.println("SETUP ------ MAPPER ----------- STEP -----------" + k + "  --------------");
		
		
		// URI to locate cachefile, ex URI a = new URI("http://www.foo.com");
		List<URI> uris = Arrays.asList(context.getCacheFiles());	
		//System.out.println("Reading cached files");
		// read cache files which contains candidates?
		for (URI uri : uris) {
			Path p = new Path(uri);
			//System.out.println("Loading " + uri.toString());
			FileSystem fs = FileSystem.get(context.getConfiguration());
			InputStreamReader ir = new InputStreamReader(fs.open(p));
			BufferedReader data = new BufferedReader(ir);
	    	while (data.ready()) {    		
	    		String line=data.readLine();
	    	//	System.out.println(line);
	    		if (line.matches("\\s*")) continue; // be friendly with empty lines
	    		// creat new prefix tempPrefix
	    		List<String> tempPrefix = new ArrayList<String>();
	    		String[] numberStrings = line.split("\\s+");
	    		for (int i = 0; i < numberStrings.length; i++){   
	    			tempPrefix.add( numberStrings[i]);
	    		}    		
	    		// add p to the list of prefix
	    		prefix.add(tempPrefix);
	    	}  
		}
		return;
   }
    
    // compare if two prefix are matched
    public static boolean matchPrefix(List<String> x, List<String> y) {
    	if (x.size() != y.size())
    		return false;
    	
    	for (int i = 0; i < x.size() - 1; i++) {
    		// we can't compare x.get(i) and y.get(i) direct, it will miss some pairs
    		String a = x.get(i);
    		String b = y.get(i);
    		if (a.compareTo(b) != 0)    			
    			return false;
    	}
    	return true;
    }
    	
    
    private void generateCandidateAndTrie(){
    	betaPrefixTrie = new Trie(-1);
     	for (int i = 0; i < prefix.size(); i++) {
    		List<String> x = prefix.get(i);
    		
    		betaPrefixTrie.addToTrie(x);
     	}
    	return; 	
    }
    
    
    
	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		//System.out.println("Mapper getting Cache");
		getCache(context); 
	
		//System.out.println("Building Trie");		
		generateCandidateAndTrie();
		
		//System.out.println("Mapper Building Hash Items");		
		buildHashItems();
		
		return;

	}

	 
	 private String itemsetToString(List<Integer> x) {
		 String a = new String();
		 a = x.get(0) + "";
		 for (int i = 1; i < x.size(); i++)
			 a = a + "\t" + x.get(i);
		 return a;
		 
	 }
	 
	 // read the Trie and output all itemsets AT LEAF
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
	//	 List<Integer> itemset = new ArrayList<Integer>();
	//	 outToReducer(context, betaPrefixTrie, itemset);
	 }
	
	 /*
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
	    		IntWritable value =  new IntWritable(nextTrie.support);
	    		context.write(key, value);
	    	}
	    	else
	    		outToReducer(context, nextTrie, nextPrefix);
	      }
	 }
	*/
	  
	 
	 // Build the Trie. Reading one transaction => Add to trie with support count
	 @Override
	 public void map(Object key, Text value, Context context
	 
	                 ) throws IOException, InterruptedException {

		 
		// each line contains a transaction with the occurences.  1 2 3    7 => transaction (1,2,3) apears 7 times
		String line = value.toString();
		
		// convert transaction to List<Integer>
		String[] s = line.split("\\s+");
		List<String> t = new ArrayList<String>();
		for(int i=0; i<s.length; i++)
		   t.add( s[i]);
		 
		// update support in Trie with transaction t
		
		List<String> p = new ArrayList<String>();
		betaPrefixTrie.prefixInTransaction = new  ArrayList<List<String>>();
		betaPrefixTrie.findingPrefix(t, p);
		
		
		// transaction t appear count times. Now we remove that number 
		int count = Integer.parseInt(  t.get(t.size()-1) );
		t.remove(t.size()-1);
		
		
		
		for (List<String> x : betaPrefixTrie.prefixInTransaction) {
			
			// x is beta prefix in the transaction
			// send to Reducers
			// Key = x
			// Value = t without x
			Text keyR = new Text(MAprioriMapperStepK.itemsetToString(x));
			
			
			List<String> tMinusX = new ArrayList<String>(t);
			
			// we will remove items that is smaller than  x.getLast
			String lastItem = x.get(x.size()-1);
			
			// we remove any items that <= lastItem

			for (Iterator<String> iterator = tMinusX.iterator(); iterator.hasNext(); ) {
				String item = iterator.next();
			    if (item.compareTo(lastItem) < 0) {   // <0 or >0 verify
			        iterator.remove();
			    }			    
			}
			
			tMinusX.removeAll(x);
			
			if (tMinusX.size() > 0) {
				Text valueR = new Text(MAprioriMapperStepK.itemsetToString(tMinusX));
				
				for (int i = 0; i < count; i++) {
					context.write(keyR, valueR);
				}
			}
		}
		
		
		

	 }
	 
	 
	}

