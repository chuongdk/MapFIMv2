package girafon.MApriori;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;




public class ReducerBetaPrefix
	extends Reducer<Text,Text,Text,Text> {

	// HashMap that contains all path of temporary file containing beta prefix
	HashMap<String, String> hashPath = new HashMap<String, String>();
	
	private int support;
	
	private Configuration config;

	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		config = context.getConfiguration();
		support = config.getInt("support", 1);
		return;
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		//System.out.println("REDUCER CLEAN UP");

		
		
	}

	 
	public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
	   
	   File tempFile = null;
	   tempFile = File.createTempFile("betaPrefix", ".tmp");
//	   System.out.println("New key : " + key.toString());
	   //System.out.println("New file : " + tempFile.getAbsolutePath());
	   BufferedWriter outFile = new BufferedWriter(new FileWriter(tempFile.getAbsolutePath(), true));

	   for (Text val : values) {
		   // with each key, I will write to a tempory file
		   outFile.write(val.toString());
		   outFile.newLine();
		   //context.write(key, val);
	   }
	   outFile.newLine();
	   outFile.close();
	   
	   // now I have a conditional database at tempFile.getAbsolutePath(). We need to call 
	   // FPF to mine it
	   List<String> commands = new ArrayList<String>();
//       commands.add("/home/chuong/workspace/Borgel/fp/fpgrowth/src/fpgrowth");
       commands.add("/home/chuong/workspace/eclat/src/eclat");
       commands.add("-ts");
       String supportBorgel = "-s-" + support;       
       commands.add(supportBorgel);
       commands.add(tempFile.getAbsolutePath());
	   

       File tempFileOutput = null;
       tempFileOutput = File.createTempFile("output", ".tmp");
       commands.add(tempFileOutput.getAbsolutePath());
       
       
//       System.out.println("Key : " + key + "\n++++++++++++++++++++++++++++++++++++++++++++++");
//       System.out.println("Command: " + commands);

       //Run macro on target
       ProcessBuilder pb = new ProcessBuilder(commands);
       pb.directory(new File("."));
       pb.redirectErrorStream(true);
       Process process = pb.start();

       //Read output
       StringBuilder out = new StringBuilder();
       BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
       String line = null, previous = null;
       while ((line = br.readLine()) != null)
           if (!line.equals(previous)) {
               previous = line;
               out.append(line).append('\n');
               //System.out.println(line);
           }

       //Check result
       if (process.waitFor() == 0) {
           //System.out.println("Success!");
           //System.out.println("----------------------------------------");
       }

       
       
       
       
       // read tempFileOutput and output to HDFS
   	FileInputStream fis = new FileInputStream(tempFileOutput);
    
   	//Construct BufferedReader from InputStreamReader
   	BufferedReader buffer = new BufferedReader(new InputStreamReader(fis));
    
   	String newline;
   	while ((newline = buffer.readLine()) != null) {
	   		context.write(key, new Text(newline));   		
   	}
       
     buffer.close();
       
	   /*
       
       //put tempFileOutput to hdfs
		 FileSystem hdfs = FileSystem.get(config);
		 Path workingDir = hdfs.getWorkingDirectory();
		 Path newFolderPath = App.getOutputForFPF(config);
		 
		 newFolderPath = Path.mergePaths(workingDir, newFolderPath);
		if(!hdfs.exists(newFolderPath))				
			hdfs.mkdirs(newFolderPath);     //Create new Directory
       
       // copy to HDFS
		System.out.println("output file : " + tempFileOutput.getAbsolutePath() + " ,size : " + tempFileOutput.length());
   		Path localFilePath = new Path(tempFileOutput.getAbsolutePath());
   		System.out.println("Copy from : " + localFilePath.toString());  		
   		Path newFilePath = new Path( newFolderPath.toString() + System.getProperty("file.separator") + tempFileOutput.getName().toString()  );
		System.out.println("Copy to : " + newFilePath.toString());

		hdfs.copyFromLocalFile(localFilePath, newFilePath);
       
      */
		tempFile.delete();
		tempFileOutput.delete();	
       
	}
}
