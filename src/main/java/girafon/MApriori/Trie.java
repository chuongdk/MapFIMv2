package girafon.MApriori;

import static com.google.common.collect.Maps.newHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;

public class Trie {
	  
	
	  private String item;
	  public int support;
	  public long data; //size of conditional database 

	  // it contains all prefix that appear in the transaction
	  public static List<List<String>> prefixInTransaction;
	  

	  
	  // this node is a leaf if children.isEmpty() = true
	  public  HashMap<String, Trie>  children = null;
	  
	  
	  public  Trie(String item){
		  this.item = item;
		  this.support = 0;
		  this.data = 0;
	  }
	  
	  
	  // item = -1
	  public  Trie(int item){
		  this.item = "NULL";
		  this.support = 0;
		  this.data = 0;
	  }	  

	  /* Finding all prefix appearing in the trasaction t
	   * Input: transaction t,  prefix p = null
	   * Output: Update prefixInTransaction 
	   */
	  public void findingPrefix(List<String> t,  List<String> p) {
		  
		  // we update prefixInTransaction only if it is a children node
		 if (children == null) {
			 // the support value is the last value of t
			 prefixInTransaction.add(p);
			 return;
		 }
		  
		 // we don't take into acount the last element
		 for (int i = 0; i < t.size()-1; i++) {
			  Trie x = children.get(t.get(i));			  
			  if (x != null) {
				  List<String> pNext = new ArrayList<String>(p);
				  pNext.add(t.get(i));
				  x.findingPrefix(t, pNext);
			  }
		  }
	  }
	  
	  
	  
	  
	  // update support of Nodes in Trie from a transaction
	  public void updateSupport(List<String> t) {
		  // we update support only if it is a leaf
		 if (children == null) {
			 // the support value is the last value of t
			 support += Integer.parseInt(  t.get(t.size()-1)  );
			 
			 // Find the position of this.item in the transaction t
			 
			 
			 data += (t.size() - t.indexOf(item) - 2) * 
					 Integer.parseInt(  t.get( t.size() - 1)  );  // - number of duplicate transaction

			 
			 
			 
//			 System.out.println(item + " " + (t.size() - t.indexOf(item) - 2) * t.get( t.size() - 1) + " support= " + support + ", data= " + data);
//			 System.out.println(t+"\n");
			 
			 return;
		 }
		  
		 // we don't take into acount the last element
		 for (int i = 0; i < t.size()-1; i++) {
			  Trie x = children.get(t.get(i));			  
			  if (x != null)
				  x.updateSupport(t);
		  }
	  }
	  

	  // add an itemset to the Trie, ex [89, 9] => new node 89	  
	  public void addToTrie(List<String> itemsets) {
		  
		  List<String> nextItemsets = new ArrayList<String>(itemsets);
		  
		  if (nextItemsets.size() == 0) return;
		  
		  if (children == null)
			  children =  newHashMap();
		  
		  String item = nextItemsets.get(0);
		  nextItemsets.remove(0);
		  
		  if (children.containsKey(item)) {
			  children.get(item).addToTrie(nextItemsets);
		  }
		  else
		  {
			  Trie  newNode = new Trie(item);
			  children.put(item, newNode);
			  
			  children.get(item).addToTrie(nextItemsets);
		  }
	  }	  
	  
	    @Override
	    public String toString() {
	      String a = new String();
	      
	      a = "Node: " + item + ", " + support +  " Child = (";
	      
	      if (children != null)
		      for (String x : children.keySet()) {
		    	Trie y = children.get(x);
		    	a = a + y.toString();
		    	a = a + " , ";
		      }
	      
	      
	      a = a + ")";
	      
	      if (!children.isEmpty()) a = a + "\n\n\n";
	      
	      return a;
	    }

	  
	  
	  
	  
}