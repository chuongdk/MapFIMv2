package girafon.MApriori;

import java.io.IOException;



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
import java.io.InputStreamReader;
import java.io.BufferedReader;


public class MAprioriMapperStepK
    extends Mapper<Object, Text, Text, Text>{
	
	
	
	// list of all prefix in the distributed cache 
    private List<List<String>> prefix = new  ArrayList<List<String>>();

    // Hasp Map of Items
    HashMap<String, Integer> hashItems = new HashMap<String, Integer>();

    private Trie candidateTrie;
    
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
		for (int i = 0; i < 10; i++)
			System.out.println("SETUP ------ MAPPER ----------- STEP -----------" + k + "  --------------");
		
		
		// URI to locate cachefile, ex URI a = new URI("http://www.foo.com");
		List<URI> uris = Arrays.asList(context.getCacheFiles());	
		System.out.println("Reading cached files");
		// read cache files which contains candidates?
		for (URI uri : uris) {
			Path p = new Path(uri);
			System.out.println("Loading " + uri.toString());
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
	    			tempPrefix.add( numberStrings[i] );
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
    	
    	candidateTrie = new Trie(-1);
 /*      	for (int i = 0; i < prefix.size(); i++) {
    		List<Integer> x = prefix.get(i);
    		for (int j = i+1; j < prefix.size(); j++) {
    			List<Integer> y = prefix.get(j);
    			
    			if (matchPrefix(x, y)) {
	    			List<Integer> tempCandidate = new ArrayList<Integer>(x);
	    			tempCandidate.add(y.get(y.size()-1));
	
	    			candidateTrie.addToTrie(tempCandidate);    			
    			}
    		}
    	}
    	*/
     	for (int i = 0; i < prefix.size(); i++) {
    		List<String> x = prefix.get(i);
    		candidateTrie.addToTrie(x);
     	}
    	
    	
    	return; 	
    }
    
    
    
	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		System.out.println("Mapper getting Cache");
		getCache(context); 
	
		System.out.println("Building Trie");		
		generateCandidateAndTrie();
		
		System.out.println("Mapper Building Hash Items");		
		buildHashItems();
		
		return;

	}

	 
	 public static String itemsetToString(List<String> x) {
		 String a = new String();
		 a = x.get(0) + "";
		 for (int i = 1; i < x.size(); i++)
			 a = a + "\t" + x.get(i);
		 return a;
		 
	 }
	 
	 // read the Trie and output all itemsets AT LEAF
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 List<String> itemset = new ArrayList<String>();
		 outToReducer(context, candidateTrie, itemset);
	 }
	 
	 public void outToReducer(Context context, Trie trie, List<String> currentPrefix) throws IOException, InterruptedException {
	      for (String x : trie.children.keySet()) {
	    	Trie nextTrie = trie.children.get(x);
	    	List<String> nextPrefix =  new ArrayList<String>();
	    	
	    	for (String z : currentPrefix)
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
		   t.add(  s[i]);
		 
		// update support in Trie with transaction t
		candidateTrie.updateSupport(t);
		

		

	 }
	 
	 
	}

