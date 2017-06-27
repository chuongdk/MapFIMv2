package girafon.MApriori;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// parameters: input output support memory #database #reducers #eclatFolder

// TODO:
// - Generate Candidates at Reducers
// - 1 phase MapReduce instead of many phase for each iteration in Apriori
// - Item ordering (0, 1, 2): 1 = increasing order, 2 = decreasing order



public class App extends Configured implements Tool {
	private static long gamma = 18;
	private static long maxDataAllow = 0;
	private long maxCandidate = 8 * 1024 * 1024;
	private int maxfullBetaPrefix = 1000; // #conditional database that we can handle
	
	private static int numberReducers = 0;
	final long DEFAULT_SPLIT_SIZE = 128  * 1024 * 1024;  // 1M
	
	private static long beginTime;
	private static long step1Time;
	private static long step2Time;
	private static long step3Time;
	private static long step4Time;
	
	private ArrayList<Path> queuePrefix = new ArrayList<Path>();
	private ArrayList<Path> queueCandidate = new ArrayList<Path>();
	
	
	// List that contain all prefix with alpha <= support < beta
    private List<List<Integer>> fullBetaPrefix = new  ArrayList<List<Integer>>();
    private List<List<Integer>> betaPrefix = new  ArrayList<List<Integer>>();
    

	private void updateQueueStep1(Configuration conf) throws IOException {
		System.out.println("Updating QUEUE after Step 1. Size queue before Update = " + queuePrefix.size());
		String sep = System.getProperty("file.separator");
		// it is output of the last step
		String output = conf.get("output") + sep + String.valueOf(conf.getInt("iteration", 1));
		Path out = new Path(output, "part-r-[0-9]*");
		FileSystem fs = FileSystem.get(conf);
		// Get all files in a directory ?
		FileStatus[] ls = fs.globStatus(out);
		for (FileStatus fileStatus : ls) {
			Path pfs = fileStatus.getPath();
			// we add to queue only non empty file
			if (getflSize(pfs.toUri().toString()) > 0)
			queuePrefix.add(pfs);		
		}
		System.out.println("DONE UPDATING QUEUE After step 1. Size queue after Update = " + queuePrefix.size());
	}
	
	private void updateQueueCandidateStep1(Configuration conf) throws IOException, InterruptedException {
		System.out.println("Updating Candidate. Size queue before Update = " + queueCandidate.size());
		String sep = System.getProperty("file.separator");
		// it is output of the last step
		String output = conf.get("output") + sep + String.valueOf(conf.getInt("iteration", 1));
		List<String> commands = new ArrayList<String>();
	    commands.add("hdfs");
	    commands.add("dfs");
	    commands.add("-getmerge");
	    output += "/part*";
	    commands.add(output);
	    File tempFile = null;
	    tempFile = File.createTempFile("output", ".tmp");
	    commands.add(tempFile.getAbsolutePath());
	    //Run macro on target
	       ProcessBuilder pb = new ProcessBuilder(commands);
	       pb.directory(new File("."));
	       pb.redirectErrorStream(true);
	       Process process = pb.start();
	       System.out.println("Command: " + commands);
	       //Check result
	       if (process.waitFor() == 0) {
	           System.out.println("Success!");
	           System.out.println("----------------------------------------");
	         System.out.println("Temp file : " + tempFile.getAbsolutePath() + " ,size : " + tempFile.length());
  			 Path localFilePath = new Path(tempFile.getAbsolutePath());
  			 System.out.println("Copy from : " + localFilePath.toString());
  			 Path newFilePath = new Path( conf.get("output") + sep + String.valueOf(conf.getInt("iteration", 1)) + System.getProperty("file.separator") + tempFile.getName().toString()  );
  			 System.out.println("Copy to : " + newFilePath.toString());

  			 FileSystem hdfs =FileSystem.get(conf);
  			 hdfs.copyFromLocalFile(localFilePath, newFilePath);
  			 tempFile.delete();			
  			 tempFile = null;
  			 prefixToCandidate(conf, newFilePath);		
			System.out.println("DONE UPDATING Candidate. Size Candidate after Update = " + queueCandidate.size());
	       }	    
	}	
	
	
	private void updateQueueCandidate(Configuration conf) throws IOException {
		System.out.println("Updating Candidate. Size queue before Update = " + queueCandidate.size());
		String sep = System.getProperty("file.separator");
		// it is output of the last step
		String output = conf.get("output") + sep + String.valueOf(conf.getInt("iteration", 1));
		Path out = new Path(output, "part-r-[0-9]*");
		FileSystem fs = FileSystem.get(conf);
		// Get all files in a directory ?
		FileStatus[] ls = fs.globStatus(out);
		for (FileStatus fileStatus : ls) {
			Path pfs = fileStatus.getPath();
			// we add to queue only non empty file
			if (getflSize(pfs.toUri().toString()) > 0)
				prefixToCandidate(conf, pfs);		
		}
		System.out.println("DONE UPDATING Candidate. Size Candidate after Update = " + queueCandidate.size());
	}
	
	
	// we put files in Queue to the Cache	
	// if remove = true then we will remove files in Queue
	private void addCacheFilesFromQueue(Configuration conf, Job job, boolean remove) throws IOException {		
		// We stop if sumSize > maxCache
		long sumSize = 0;		
		int count = 0;
		if (remove == true)
			while ((sumSize < maxCandidate) && (queuePrefix.size() > 0)) {
				count++;
				Path pfs = queuePrefix.get(0);
				System.out.println("Adding " + pfs.toUri().toString());
				job.addCacheFile(pfs.toUri());			
				sumSize += getflSize(pfs.toUri().toString());			

				queuePrefix.remove(0);
			}
		// adding ALL but not remove the quest, it is for step 1B
		else {
			for (Path pfs:queuePrefix) {
				System.out.println("Adding " + pfs.toUri().toString());
				job.addCacheFile(pfs.toUri());
			}
		}
		
		System.out.println("ADDED   " + count + "  FILES to CACHED, Sum Size = " + sumSize);		
	}	
	
	
	// we put files in Queue Candidate to the Cache	
	// if remove = true then we will remove files in Queue
	private void addCacheFilesFromQueueCandidate(Configuration conf, Job job) throws IOException {		
		// We stop if sumSize > maxCache
		long sumSize = 0;		
		int count = 0;
		while ((sumSize < maxCandidate) && (queueCandidate.size() > 0)) {
			count++;
			Path pfs = queueCandidate.get(0);
			System.out.println("Adding " + pfs.toUri().toString());
			job.addCacheFile(pfs.toUri());			
			sumSize += getflSize(pfs.toUri().toString());			
			queueCandidate.remove(0);
		}
		
		System.out.println("ADDED   " + count + "  FILES to CACHED, Sum Size = " + sumSize);		
	}	
	
	
	
	// return file size in the HDFS  (in BYTES). Our Limit will be 100000	
    public long getflSize(String args) throws IOException, FileNotFoundException
    {
        Configuration config = new Configuration();
        Path path = new Path(args);
        FileSystem hdfs = path.getFileSystem(config);
        ContentSummary cSummary = hdfs.getContentSummary(path);
        long length = cSummary.getLength();
        return length;
    }
    
	
	// we will output to Output/1,2,3,4
	private Path getOutputPath(Configuration conf, int iteration) {
		String sep = System.getProperty("file.separator");
		System.out.println("Uls"
				+ "sing output " + conf.get("output") + sep + String.valueOf(iteration));
		
		return new Path(conf.get("output") + sep + String.valueOf(iteration));
	}
	
	// we will output to Output/data
	private Path getOutputPathCompressData(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + "compressedData");
	}
	
	
	// we will output to Output/fullBetaPrefix
	private Path getOutputPathfullBetaPrefix(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "fullBetaPrefix");
	}
	
	static public Path getOutputForFPF(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "fromFPF");
	}
	
	private Path getInputPathCompressData(Configuration conf) {
		String sep = System.getProperty("file.separator");
		System.out.println("Using input " + conf.get("output") + sep + "compressedData");
		return new Path(conf.get("output") + sep + "compressedData");
	}
	
	
	private Path getCandidatePath(Configuration conf) throws IOException {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "candidate" + sep);
	}
	
	private Path getCandidatePathWithIteration(Configuration conf) throws IOException {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "candidate" + sep + conf.getInt("iteration", 1));
	}
	
	
	private Path getInputPath(Configuration conf) {
		System.out.println("Using input " + conf.get("input"));
		return new Path(conf.get("input"));
	}
	
	
	//  itemset (1, 2, 3) =>  1_2_3_
	// itemset (1) => 1_
	private String prefixToString(List<Integer> p) {
		String a = new String("_");
		for (int i: p)
			a = a + i + "_";		
		return a;
	}
	
	// candidate (1, 2, 3, 4) => string "1 2 3 4"
	private String candidateToString(List<Integer> p) {
		String a = new String();
		for (int i = 0; i < p.size()-1; i++)
			a += p.get(i) + " ";
		a += p.get(p.size()-1);
		return a;
	}	
	
	// We read a prefix File, and generate many candidate file to the HDFS
	// we take into account beta 
	
	
	private void prefixToCandidate(Configuration conf, Path prefixFile) throws IOException {
		// we generate new candidate only if both parents are >= beta
		int beta = conf.getInt("beta", 0);
		int support = conf.getInt("support", 0);
		
		// create candidate directory in the hdfs
		 FileSystem hdfs =FileSystem.get(conf);
		 Path workingDir=hdfs.getWorkingDirectory();
		 Path newFolderPath= getCandidatePath(conf);
		 newFolderPath=Path.mergePaths(workingDir, getCandidatePath(conf));
		if(!hdfs.exists(newFolderPath))				
			hdfs.mkdirs(newFolderPath);     //Create new Directory
		newFolderPath=Path.mergePaths(workingDir, getCandidatePathWithIteration(conf));
		if(!hdfs.exists(newFolderPath))				
			hdfs.mkdirs(newFolderPath);     //Create new Directory
		System.out.println("Loading prefix file: " + prefixFile.toString());
		FileSystem fs = FileSystem.get(conf);
		InputStreamReader ir = new InputStreamReader(fs.open(prefixFile));
		BufferedReader data = new BufferedReader(ir);
		List<List<Integer>> prefix = new  ArrayList<List<Integer>>();
		List<Boolean> checkBeta = new ArrayList<Boolean>();
    	while (data.ready()) {    		
    		String line=data.readLine();
    		if (line.matches("\\s*")) continue; // be friendly with empty lines
    		// creat new prefix tempPrefix
    		String[] numberStrings = line.split("\\s+");
			List<Integer> tempPrefix = new ArrayList<Integer>();
    		for (int i = 0; i < numberStrings.length - 2; i++){   // we don't get the last two one as it is the frequent + data size
    			tempPrefix.add(Integer.parseInt(numberStrings[i]));
    		}
    		// we sort tempPrefix in order 1 < 2 ...
	    	// we only add tempPrefix if support >= beta
    		// we add all prefix, even if < beta.
    		prefix.add(tempPrefix);
    		if (  Long.parseLong(numberStrings[ numberStrings.length-1  ]) <= maxDataAllow) {	// if data size <= maxDataAllow	
    			// we mine beta if and only if DataSize >= Support
    			if (Long.parseLong(numberStrings[ numberStrings.length-1  ]) >= support) 
	    			fullBetaPrefix.add(tempPrefix);
    			
    			checkBeta.add(false);    // false => < beta
	    	}
    		else 
    			checkBeta.add(true);	// true =>  > beta
    	}  		
    	File tempFile = null;
    	// now we have all prefixes in the variable prefix. Our job is to generate candidate files
       	for (int i = 0; i < prefix.size(); i++) {
    		List<Integer> x = prefix.get(i);
    		if (checkBeta.get(i) == true) 
    		// we check beta here, We take only x with support >= beta.
    		// SO x >= beta, and Y can be < beta 
    		{
	    		// we will not create new file if it is note too big
	    		if (tempFile == null)
	    			tempFile = File.createTempFile(prefixToString(x), ".tmp");
	    		BufferedWriter outFile = new BufferedWriter(new FileWriter(tempFile.getAbsolutePath(), true));
	    		for (int j = 0; j < prefix.size(); j++) {
	    			List<Integer> y = prefix.get(j);
	    			// we do it only if x < y
		    			if (MAprioriMapperStepK.matchPrefix(x, y)) {
		    				// check if x < y
		    				int a = x.get(x.size() - 1);
		    				int b = y.get(y.size() - 1);
		    				if (a < b) {
				    			List<Integer> tempCandidate = new ArrayList<Integer>(x);
				    			tempCandidate.add(y.get(y.size()-1));
				    			Collections.sort(tempCandidate);
				    			outFile.write( candidateToString(tempCandidate) );
				    			outFile.newLine();
				    		//	System.out.println(tempCandidate + "    " + candidateToString(tempCandidate));
				    		//	System.out.println("AB : " +a  + " " + b);
				    			
		    				}
		    			}
	    			
	    		}
	    		outFile.close();
	    		// send tempFile to HDFS candidate if it is larger than limit
	    		if (tempFile.length() > maxCandidate) {
	    			 System.out.println("Temp file : " + tempFile.getAbsolutePath() + " ,size : " + tempFile.length());
	    			 System.out.println(newFolderPath.toString());    			
	    			 Path localFilePath = new Path(tempFile.getAbsolutePath());
	    			 System.out.println("Copy from : " + localFilePath.toString());
	    			 Path newFilePath = new Path( newFolderPath.toString() + System.getProperty("file.separator") + tempFile.getName().toString()  );
	    			 System.out.println("Copy to : " + newFilePath.toString());
	    			 hdfs.copyFromLocalFile(localFilePath, newFilePath);
	    			 tempFile.delete();			
	    			 tempFile = null;
	    			 queueCandidate.add(newFilePath);
	    		}    		
    		}
    	}   
       	if ((tempFile != null) && (tempFile.length() > 0)){
       		System.out.println("Temp file : " + tempFile.getAbsolutePath() + " ,size : " + tempFile.length());
			 Path localFilePath = new Path(tempFile.getAbsolutePath());
			 System.out.println("Copy from : " + localFilePath.toString());
			 Path newFilePath = new Path( newFolderPath.toString() + System.getProperty("file.separator") + tempFile.getName().toString()  );
			 System.out.println("Copy to : " + newFilePath.toString());
			 hdfs.copyFromLocalFile(localFilePath, newFilePath);
			 tempFile.delete();			
			 tempFile = null;     
			 queueCandidate.add(newFilePath);
       	}
	}
	
	
	private void betaPrefixToHDFS(Configuration conf, Job job) throws IOException {
		
		// create candidate directory in the hdfs
		 FileSystem hdfs = FileSystem.get(conf);
		 Path workingDir = hdfs.getWorkingDirectory();
		 Path newFolderPath = getOutputPathfullBetaPrefix(conf);
		 newFolderPath = Path.mergePaths(workingDir, getOutputPathfullBetaPrefix(conf));
		if(!hdfs.exists(newFolderPath))				
			hdfs.mkdirs(newFolderPath);     //Create new Directory
    	
    	File tempFile = null;
    	// now we have all beta prefixes. Our job is to save to HDFS
       	tempFile = File.createTempFile("betaPrefix", ".tmp");
       	BufferedWriter outFile = new BufferedWriter(new FileWriter(tempFile.getAbsolutePath(), true));

       	
       	for (List<Integer> tempCandidate: betaPrefix) {
       				outFile.write( candidateToString(tempCandidate) );
	    			outFile.newLine();
    	}
    	outFile.close();

    	System.out.println("Beta Prefix file : " + tempFile.getAbsolutePath() + " ,size : " + tempFile.length());
   		Path localFilePath = new Path(tempFile.getAbsolutePath());
   		System.out.println("Copy from : " + localFilePath.toString());  		
   		Path newFilePath = new Path( newFolderPath.toString() + System.getProperty("file.separator") + tempFile.getName().toString()  );
		System.out.println("Copy to : " + newFilePath.toString());
		hdfs.copyFromLocalFile(localFilePath, newFilePath);
		tempFile.delete();			
		tempFile = null;     
		job.addCacheFile(newFilePath.toUri());		   			
		
	}
		

	
	
	
	
	Configuration setupConf(String[] args, int iteration) {
		Configuration conf = new Configuration();
		conf.set("input", args[0]);  
		conf.set("output", args[1]); 
		conf.set("eclatFolder", args[6]);
		conf.setInt("maxTrieSize", Integer.valueOf(args[7]));
		conf.setLong("maxDataAllow", maxDataAllow);
		conf.setInt("support", Integer.valueOf(args[2]));
		conf.setInt("beta", Integer.valueOf(args[3])); // beta threshold for DApriori
				conf.setInt("iteration", iteration);  // first step that finding all frequent itemset

		conf.setLong(
			    FileInputFormat.SPLIT_MAXSIZE,
			    DEFAULT_SPLIT_SIZE);
		
		return conf;
	}
	
	Job setupJobStep1(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "SFIM step 1");
		// Configure Jobs to be run on local => We can println to screen to DEBUG
		//conf.set("mapred.job.tracker", "local"); 			
		job.setJarByClass(App.class);
		job.setMapperClass(MAprioriMapper.class);
		job.setCombinerClass(Combiner.class);
		//job.setReducerClass(MAprioriReducer.class);
		job.setReducerClass(ReducerGlobalPhase.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(numberReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, getInputPath(conf));
		FileOutputFormat.setOutputPath(job, getOutputPath(conf, 1));// output path for iteration 1 is: output/1
		
		return job;
	}
	

	Job setupJobStep1B(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "compress Data");
		job.setJarByClass(App.class);
		job.setMapperClass(MAprioriMapperCompressData.class);
		
		job.setCombinerClass(MAprioriReducer.class);
		job.setReducerClass(MAprioriReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
			
		addCacheFilesFromQueue(conf, job, false);
		// set number of reducers
		job.setNumReduceTasks(numberReducers);

			
		FileInputFormat.addInputPath(job, getInputPath(conf));
		FileOutputFormat.setOutputPath(job, getOutputPathCompressData(conf));// output path for iteration 1 is: output/1
		return job;
	}

	Job setupJobStep2(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "MapFIM step 2");
		job.setJarByClass(App.class);
		//job.setMapperClass(MAprioriMapperStepK.class);
		job.setMapperClass(MapperGlobalPhase.class);
		//we need a custom partitioner, so each reducer take care of the same set of prefixs
		job.setPartitionerClass(HashPartitioner.class);

		job.setReducerClass(ReducerGlobalPhase.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		job.setNumReduceTasks(numberReducers);
		
		addCacheFilesFromQueueCandidate(conf, job);
					
		FileInputFormat.addInputPath(job, getInputPathCompressData(conf));
		FileOutputFormat.setOutputPath(job, getOutputPath(conf, conf.getInt("iteration", 1)));// output path for iteration 1 is: output/1
		return job;
	}
	
	Job setupJobStep3(Configuration conf) throws Exception {	
		Job job = Job.getInstance(conf, "MapFIM Mining Prefix");
		job.setJarByClass(App.class);
		job.setMapperClass(MapperBetaPrefix.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
		betaPrefixToHDFS(conf, job);
				
		job.setNumReduceTasks(numberReducers);
		job.setReducerClass(ReducerBetaPrefix.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, getInputPathCompressData(conf));
		FileOutputFormat.setOutputPath(job, getOutputPath(conf, conf.getInt("iteration", 1)));
		return job;
	}

	public static int countLines(String filename) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
	
	public int run(String[] args) throws Exception {
		maxfullBetaPrefix = Integer.parseInt(args[4]);
		
		// Iteration 1
		{
			Configuration conf = setupConf(args, 1);
			Job job = setupJobStep1(conf);
			job.waitForCompletion(true);	
			// Queue Prefix is updated when We have output
			updateQueueStep1(conf);
			updateQueueCandidateStep1(conf);
		}
		step1Time = System.currentTimeMillis();
		
		// add a step to compress database.
		// We delete every transactions that don't contains at least 2 frequent item in the output/1
	
		// Iteration 1b - to compress data 
		{
			// Now, Queue contains all frequent items

			System.out.println("__________________STEP 1B _____________________");
			System.out.println("__________________STEP 1B _____________________");
			
			Configuration conf = setupConf(args, 0);
			conf.setInt("support", 1);  // because we compress data
			Job job = setupJobStep1B(conf);
			job.waitForCompletion(true);
		}		
		step2Time = System.currentTimeMillis();
		
		// Queue contains prefix files => we will get it and create candidate files in the hdfs
		// then we need other queue for  candidate file

		// Now I need to put output to Distributed Cache
		int iteration = 2;
		boolean stop = false;
		if (queueCandidate.size() == 0) {
			stop = true;
		}
		while (!stop) {
			System.out.println("_____STEP " + iteration + "  _______");
			System.out.println("_____STEP " + iteration + "  _______");
			
			Configuration conf = setupConf(args, iteration);
			
			Job job = setupJobStep2(conf);
		
			job.waitForCompletion(true);
			
			
			System.out.println("\n\n\n\nStill having Candidate? : " + hasCandidate(conf, iteration));
			// the number of iteration is not the iteration of steps, but the iteration of mapper/reducer
			// each time, we will load every output of the last iteration, the load it to the QUEUE
			// put some files in the QUEUE to the CACHE => make sure the sum is less than <10000
			// queue is updated right after we have output
			//updateQueue(conf);
			stop = !hasCandidate(conf, iteration);

			
			updateQueueCandidate(conf);
			iteration++;

			

		}
		step3Time = System.currentTimeMillis();
		
		// Last step, Mining fullBetaPrefix
		// number of reducers = number of fullBetaPrefix
		
		// each time we will send maxfullBetaPrefix to Mappers
		// maxBetaPrefix = 1000;  fullBetaPrefix.size() = 5000 => we send to 5 times
		// fullBetaPrefix.size() = 5001 => we send to 6 times
		
		int nIteration = fullBetaPrefix.size() / maxfullBetaPrefix;
		if (fullBetaPrefix.size() % maxfullBetaPrefix != 0)
			nIteration++;

		System.out.println("------------------ Mining beta Prefix  #beta Prefix = " +fullBetaPrefix.size());
		
		for (int i = 0; i < nIteration; i++) 	{		
			System.out.println("------------------ Mining beta Prefix ---------------STEP " + (i+1) + "/" + nIteration);
			System.out.println("------------------ Mining beta Prefix ---------------STEP " + (i+1) + "/" + nIteration);
			
			// in Iteration i, we collect prefix from  i * 10000 to (i+1)*1000 - 1;
			betaPrefix = new  ArrayList<List<Integer>>();
			for (int j = i * maxfullBetaPrefix; j < (i+1) * maxfullBetaPrefix; j++) {
				if (j < fullBetaPrefix.size()) {
					// copy fullBetaPrefix(j) to betaPrefix
					betaPrefix.add(fullBetaPrefix.get(j));
				}
			}
			
			Configuration conf = setupConf(args, iteration);
			
			/* step 1. Put the fullBetaPrefix to the distributed cache
			* Note: We don't need to put all beta Prefix, because number of reducers
			* size of beta prefix. So we may want to partition it for several times.
			*/
			/* step 2. Mappers: read beta Prefix, for each transaction
			 * for all Prefix:
			 * if transaction t contains prefix [a, b, c] then
			 * we  t - [a, b, c] = t'
			 * output  to reducer key = [a, b, c], value = t'
			 * So, 1 transaction can output to many pair key, value
			 */

			
			/* step 3. Reducers mine locally and report the result
			 * with each key, value, it save the transaction locally to HDD 
			 * to temporary file:  _tmp_Key
			 * Then at the end, function cleanup is called. 
			 * It mine locally and report back the result
			 * 
			 */
			
			//we have to partition beta Prefix into pack of 10k prefix for ex.
			
			Job job = setupJobStep3(conf);
			job.waitForCompletion(true);
			iteration++;
		}
		
		iteration--;
		Configuration conf = setupConf(args, iteration);
		printResult(conf, iteration, args);

		return 1;
	}
	
	
	// export Result to the disk
	public void printResult(Configuration conf, int k, String args[]) throws IOException, InterruptedException {
		System.out.println("Print result");
		String sep = System.getProperty("file.separator");
		List<String> commands = new ArrayList<String>();
	    commands.add("hdfs");
	    commands.add("dfs");
	    commands.add("-getmerge");
	    
	    for (Integer i = 1; i <= k; i++) {
	    	String output = conf.get("output") + sep + i.toString();  
	    	output += "/part*";
	    	commands.add(output);
	    }
	    
	    String outputFile = "result_" + args[0].substring(6) + "_support_" +    args[2] + ".txt";
    	commands.add(outputFile);
 
    	
	    //Run macro on target
       ProcessBuilder pb = new ProcessBuilder(commands);
       pb.directory(new File("."));
       pb.redirectErrorStream(true);
       Process process = pb.start();
       
       System.out.println("Command: " + commands);
       //Check result
       if (process.waitFor() == 0) {
           System.out.println("Success!\n");           
       }	           
	           		
	}	
	
	private boolean hasCandidate(Configuration conf, Integer iteration) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		Path candidatePath = new Path(conf.get("output") 
				+ System.getProperty("file.separator")  
				+ iteration.toString() 
				+ System.getProperty("file.separator")
				+ "candidate" );	
		 
		return fs.exists(candidatePath);
		
	}
	
	
	public static void main(String[] args) throws Exception {
		numberReducers = Integer.parseInt(args[5]);
		maxDataAllow = Long.parseLong(args[3]) * 1024 /gamma * 1024;
		
		
		System.out.println("----------------------------RUNNING-------------------------");
		System.out.println("Input            : " + args[0]);
		System.out.println("Output           : " + args[1]);
		System.out.println("Support          : " + args[2]);		
		System.out.println("Max Memory Allow : " + args[3]);	
		System.out.println("Max Data Allow   : " + maxDataAllow);		
		System.out.println("#Databases       : " + args[4]);
		System.out.println("#Reducer         : " + args[5]);
		System.out.println("Eclat Folder     : " + args[6]);
		System.out.println("max Trie size    : " + args[7]);

		System.out.println("------------------------------------------------------------");		
		
		beginTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new App(), args);
		step4Time = System.currentTimeMillis();


		System.out.println("----------------------------SUMARRY-------------------------");
		System.out.println("Input            : " + args[0]);
		System.out.println("Output           : " + args[1]);
		System.out.println("Support          : " + args[2]);		
		System.out.println("Max Memory Allow : " + args[3]);	
		System.out.println("Max Data Allow   : " + maxDataAllow);		
		System.out.println("#Databases       : " + args[4]);
		System.out.println("#Reducer         : " + args[5]);
		System.out.println("Eclat Folder     : " + args[6]);
		System.out.println("max Trie size    : " + args[7]);		
	    String outputFile = "result_" + args[0].substring(6) + "_support_" +    args[2] + ".txt";
	    System.out.println("Number of FIMs   : " + countLines(outputFile));
		System.out.println("------------------------------------------------------------");		

		
		System.out.println("Step 1 time  : " + (step1Time - beginTime)/1000 + " seconds.");
		System.out.println("Step 1b time : " + (step2Time - step1Time )/1000 + " seconds.");
		System.out.println("Step 2 time  : " + (step3Time - step2Time )/1000 + " seconds.");
		System.out.println("Step 3 time  : " + (step4Time - step3Time )/1000 + " seconds.");
		System.out.println("Total time   : " + (step4Time - beginTime)/1000 + " seconds.");
		
		System.exit(exitCode);
	}

}















