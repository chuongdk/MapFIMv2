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
    private List<List<String>> fims; 
    private List<List<String>> betaFims;
    
    List<String> prevFim = null;
    
    private Path candidatePath;
    private Path betaPath;
    
    private Configuration conf;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		support = conf.getInt("support", 1);
		maxDataAllow = conf.getLong("maxDataAllow", 0);
		fims = new  ArrayList<List<String>>();
		betaFims = new  ArrayList<List<String>>();
		
		 
		candidatePath =  new Path(conf.get("output") + 
				System.getProperty("file.separator") + 
				conf.get("iteration") +  System.getProperty("file.separator")  + 
				"candidate");
		betaPath =  new Path(conf.get("output") + 
				System.getProperty("file.separator")  + 
				"betaFIMs");
		
		return;
	}
	
	
	// key are solved in order, so we are sure that key with k-1 prefix are 
	// solved together
	
	private boolean sharePrefix(List<String> a, List<String> b) {
		if (a == null)
			return true;
		
		// check if they share k-1 prefix
		for (int i = 0; i < a.size() - 2; i++){
			//if (a.get(i) != b.get(i))
			if (a.get(i).compareTo(b.get(i)) != 0)
				return false;
		}
		
		return true;
	}
	
	// generate Candidates in fims to HDFS
	// m = fims.size() => we generate m-1 + m-2 + ... + 1 candidates
	
	// ganerate: have to take care of DATA
	private void generateCandidate() throws IOException {

		//System.out.println("\n\nNew Bucket");
		//for (int i = 0; i < fims.size(); i++)
		//	System.out.println(fims.get(i));
		
		if (fims.size() <= 1)
			return;
					
		// create new temp file
		List<String> firstFim = fims.get(0);
		StringBuilder fileName = new StringBuilder("_Candidate_");
		for (int i = 0; i < firstFim.size()-2; i++)
			fileName.append( firstFim.get(i).toString() + "_" );
		
		File tempFile = File.createTempFile(fileName.toString(), ".tmp");
		//System.out.println("Creating a new temp file " + tempFile.getAbsolutePath());
		BufferedWriter outFile = new BufferedWriter(new FileWriter(tempFile.getAbsolutePath(), true));		

		long count = 0;
		for (int i = 0; i < fims.size(); i++) {
			List<String> a = fims.get(i);

			// generate only candidate with flag = 1
			if (a.get(a.size()-1).compareTo(new String("FLAG")) == 0)
				for (int j = 0; j < fims.size(); j++) {
	
					List<String> b = fims.get(j);
					
					// if a < b
					if (a.get(a.size()-2).compareTo( b.get(b.size()-2)) < 0) {
						List<String> candidate;
						
						candidate = new ArrayList<String>(a);
						
						// remove the flag (last position)
						candidate.remove(candidate.size()-1);
						
						candidate.add(b.get(b.size()-2));
					
						StringBuilder stringCandiate = new StringBuilder("");
						for (String item : candidate){
							stringCandiate.append(item.toString() + "\t");
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
	   		//System.out.println("CANDIDATE Copy from : " + localFilePath.toString());  		
			//System.out.println("CANDIDATE Copy to : " + candidatePath.toString());
			
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
		   List<String> aFIM = new ArrayList<String>();
		   for (String item: splits) {
			   aFIM.add(item);
		   }
		   
		   // last item of aFIM is a flag. 
		   // Flag = 1 if it pass maxDataAllow
		   if (sumData >= maxDataAllow) {
			   aFIM.add(new String("FLAG"));
		   }
		   else   { 
			   aFIM.add("OK");
			   
			   // add it to betaFIMs if Data >= support. Because if Data < support, we don't have to mine conditional data
			   if (sumData >= support) {
			   // add aFIM to betaData
				   	betaFims.add(aFIM);
			   }
			   

		   }
		   
		   
		   // add aFIM to fims if prevFim is null or prevFIM share k-1 prefix
		   if (sharePrefix(prevFim, aFIM)){
			   fims.add(aFIM);
			   prevFim = new ArrayList<String>(aFIM);
		   }
		   else
		   {
			   // generate candidates
			   generateCandidate();
			   // reset fims
			   fims = new  ArrayList<List<String>>();
			   fims.add(aFIM);
			   prevFim = new ArrayList<String>(aFIM);
		   }
		   

	   }
	   
	}
	
	// Generate Candidates from FIMs
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		//System.out.println("\n\n\nGenerate the last bucket");
		generateCandidate();
		
		// Save the beta FIM to HDFS
		System.out.println("-------------BETA FIMS------------------");
		if (betaFims.size() > 0) {
			StringBuilder fileName = new StringBuilder("beta_");
			// add task ID
			fileName.append(conf.get("mapred.task.id", "1") + "_");
			
			File tempFile = File.createTempFile(fileName.toString(), ".tmp");
			//System.out.println("Creating a new temp file " + tempFile.getAbsolutePath());
			BufferedWriter outFile = new BufferedWriter(new FileWriter(tempFile.getAbsolutePath(), true));		

			// write beta FIMs to file
			for (List<String> x : betaFims) {
				for (int i = 0; i < x.size()-1; i++)
					outFile.write( x.get(i) + "\t");				
    			outFile.newLine();
			}
			
			outFile.close();
			// put tempFile to the HDFS at output/betaPath
		   	Path localFilePath = new Path(tempFile.getAbsolutePath());
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.copyFromLocalFile(localFilePath, 
					new Path(betaPath.toString() + 
							System.getProperty("file.separator") + 
							tempFile.getName()));
			tempFile.delete();		
		}
 		
	}
	
	
}
