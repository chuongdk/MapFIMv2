package girafon.MApriori;


import org.apache.hadoop.mapreduce.Partitioner;



public class HashPartitioner<K, V> extends Partitioner<K, V> {

	@Override
	public int getPartition(K arg0, V arg1, int arg2) {
		// TODO Auto-generated method stub
	
		// key is a list of items in a prefix, 
		String key = arg0.toString();
		
		// now get the list
		
		String[] numberStrings = key.split("\t");
		
		// newKey is the sum of all items
		int newKey = 0;
		for (int i = 0; i < numberStrings.length - 1; i++){   // we don't get the last one as it is the frequent
			newKey = newKey +  Integer.parseInt(numberStrings[i]);
		}
		
				
		return newKey % arg2;
	}
  
}