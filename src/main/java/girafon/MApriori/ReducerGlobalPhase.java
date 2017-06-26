package girafon.MApriori;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;




// Compute global support => get Lk => Generate Ck+1 (in clean up) 
public class ReducerGlobalPhase 
	extends Reducer<Text,Text,Text,Text> {
 
	private int support;
	// list of FIMs
    private List<List<Integer>> fims; 

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		support = config.getInt("support", 1);
		fims = new  ArrayList<List<Integer>>();
		return;
	}
	
	public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
 
	   int sumSupport = 0;
	   long sumData = 0;
	   for (Text val : values) {
		   String[] splits = val.toString().split("\\s+");
		   
	     sumSupport += Integer.parseInt(splits[0]);
	     if (splits.length > 1)
	    	 sumData += Long.parseLong(splits[1]);
	     
	   }
	   
	   String exportValue = sumSupport + " " + sumData;
	   if (sumSupport >= support) {
		   context.write(key, new Text(exportValue));
		   // save FIMs to the memory
		   String[] splits = key.toString().split("\\s+");
		   List<Integer> aFIM = new ArrayList<Integer>();
		   for (String item: splits) {
			   aFIM.add(Integer.parseInt(item));
		   }
		   fims.add(aFIM);
	   }
	   
	}
	
	// Generate Candidates from FIMs
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("\n\n\n+++++++++++++++++++++++++++++++");
		for (List<Integer> aFIM : fims) {
			System.out.println(aFIM);
		}
		
		
	}
	
	
}
