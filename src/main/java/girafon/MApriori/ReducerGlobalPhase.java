package girafon.MApriori;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;




// Compute global support => get Lk => Generate Ck+1 (in clean up) 
public class ReducerGlobalPhase 
	extends Reducer<Text,Text,Text,Text> {
 
	private int support;
	private long maxDataAllow;
	// list of FIMs
    private List<List<Integer>> fims; 
    List<Integer> prevFim = null;
    
    private Path candidatePath;
    private Configuration conf;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		support = conf.getInt("support", 1);
		maxDataAllow = conf.getLong("maxDataAllow", 0);
		fims = new  ArrayList<List<Integer>>();
		
		
		
		candidatePath =  new Path(conf.get("output") + 
				System.getProperty("file.separator") + 
				conf.get("iteration") +  System.getProperty("file.separator")  + 
				"candidate");
		return;
	}
	
	
	// key are solved in order, so we are sure that key with k-1 prefix are 
	// solved together
	
	private boolean sharePrefix(List<Integer> a, List<Integer> b) {
		if (a == null)
			return true;
		
		// check if they share k-1 prefix
		for (int i = 0; i < a.size() - 2; i++){
			if (a.get(i) != b.get(i))
				return false;
		}
		
		return true;
	}
	
	// generate Candidates in fims to HDFS
	// m = fims.size() => we generate m-1 + m-2 + ... + 1 candidates
	
	// ganerate: have to take care of DATA
	private void generateCandidate() throws IOException {

		System.out.println("\n\nNew Bucket");
		for (int i = 0; i < fims.size(); i++)
			System.out.println(fims.get(i));
		
		if (fims.size() <= 1)
			return;
		

			
			
			
		// create new temp file
		List<Integer> firstFim = fims.get(0);
		StringBuilder fileName = new StringBuilder("_Candidate_");
		for (int i = 0; i < firstFim.size()-2; i++)
			fileName.append( firstFim.get(i).toString() + "_" );
		
		File tempFile = File.createTempFile(fileName.toString(), ".tmp");
		System.out.println("Creating a new temp file " + tempFile.getAbsolutePath());
		BufferedWriter outFile = new BufferedWriter(new FileWriter(tempFile.getAbsolutePath(), true));		

		long count = 0;
		for (int i = 0; i < fims.size(); i++) {
			List<Integer> a = fims.get(i);

			// generate only candidate with flag = 1
			if (a.get(a.size()-1) == 1)
				for (int j = 0; j < fims.size(); j++) {
	
					List<Integer> b = fims.get(j);
					if (a.get(a.size()-2) < b.get(b.size()-2)) {
						List<Integer> candidate;
						
						candidate = new ArrayList<Integer>(a);
						
						// remove the flag (last position)
						candidate.remove(candidate.size()-1);
						
						candidate.add(b.get(b.size()-2));
					
						StringBuilder stringCandiate = new StringBuilder("");
						for (Integer item : candidate){
							stringCandiate.append(item.toString() + " ");
						}
						// save candidate to file
						outFile.write( stringCandiate.toString() );
		    			outFile.newLine();
		    			count++;
					}
				}
		}
		outFile.close();
		
		// put tempFile to the HDFS at output/candidate
		if (count > 0) {
	   		Path localFilePath = new Path(tempFile.getAbsolutePath());
	   		System.out.println("CANDIDATE Copy from : " + localFilePath.toString());  		
			System.out.println("CANDIDATE Copy to : " + candidatePath.toString());
			
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.copyFromLocalFile(localFilePath, new Path(candidatePath.toString() + System.getProperty("file.separator") + tempFile.getName()));
		}
		tempFile.delete();			
		
		
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
		   
		   // last item of aFIM is a flag. 
		   // Flag = 1 if it pass maxDataAllow
		   if (sumData > maxDataAllow) {
			   aFIM.add(1);
		   }
		   else
			   aFIM.add(0);
		   
		   
		   // add aFIM to fims if prevFim is null or prevFIM share k-1 prefix
		   if (sharePrefix(prevFim, aFIM)){
			   fims.add(aFIM);
			   prevFim = new ArrayList<Integer>(aFIM);
		   }
		   else
		   {
			   // generate candidates
			   generateCandidate();
			   // reset fims
			   fims = new  ArrayList<List<Integer>>();
			   fims.add(aFIM);
			   prevFim = new ArrayList<Integer>(aFIM);
		   }
		   

	   }
	   
	}
	
	// Generate Candidates from FIMs
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("\n\n\nGenerate the last bucket");
		generateCandidate();
		
		
	}
	
	
}
