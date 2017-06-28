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
// - Reducers generate file containning beta FIM (FIM with data <= maxDataAllow)
// => at output/betaFIMs
// - Rewrite step4
// - Item ordering (0, 1, 2): 1 = increasing order, 2 = decreasing order



public class App extends Configured implements Tool {
	private static long gamma = 18;
	private static long maxDataAllow = 0;
 
	private int maxfullBetaPrefix = 1000; // #conditional database that we can handle
	
	private static int numberReducers = 0;
	final long DEFAULT_SPLIT_SIZE = 128  * 1024 * 1024;  // 1M
	
	private static long beginTime;
	private static long step1Time;
	private static long step2Time;
	private static long step3Time;
	private static long step4Time;
	

	
	// we will output to Output/1,2,3,4
	private Path getOutputPath(Configuration conf, int iteration) {
		String sep = System.getProperty("file.separator");
		System.out.println("Using output " + conf.get("output") + sep + String.valueOf(iteration));
		return new Path(conf.get("output") + sep + String.valueOf(iteration));
	}
	
	// we will output to Output/data
	private Path getOutputPathCompressData(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + "compressedData");
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
	
	
	
	private Path getInputPath(Configuration conf) {
		System.out.println("Using input " + conf.get("input"));
		return new Path(conf.get("input"));
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
		job.setMapperClass(MapperCompressData.class);
		
		job.setCombinerClass(MAprioriReducer.class);
		job.setReducerClass(MAprioriReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
			
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
			iteration++;

		}
		step3Time = System.currentTimeMillis();
		
		// Last step, Mining fullBetaPrefix

		System.out.println("------------------ Mining beta Prefix--------------------");
		
/*		for (int i = 0; i < nIteration; i++) 	{		
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
		/*	
			Job job = setupJobStep3(conf);
			job.waitForCompletion(true);
			iteration++;
		}
		
		iteration--;*/
		Configuration conf = setupConf(args, iteration);
		printResult(conf, iteration, args);
		getAllBetaFIMs(conf, args);
		
		return 1;
	}
	
	
	// export Result to the disk
	public void getAllBetaFIMs(Configuration conf, String args[]) throws IOException, InterruptedException {
		System.out.println("Print result");
		String sep = System.getProperty("file.separator");
		List<String> commands = new ArrayList<String>();
	    commands.add("hdfs");
	    commands.add("dfs");
	    commands.add("-getmerge");
	    String output = conf.get("output") + sep + "betaFIMs/*";
	    commands.add(output);
	    String outputFile = "betaFIMs_" + args[0].substring(6) + "_support_" +    args[2] + ".txt";
    	commands.add(outputFile);
 
	    //Run macro on target
    	ProcessBuilder pb = new ProcessBuilder(commands);
	    pb.directory(new File("."));
	    pb.redirectErrorStream(true);
	    Process process = pb.start();
	    System.out.println("Command: " + commands);
	    //Check result
	    if (process.waitFor() == 0) 
	        System.out.println("Success!\n");
	    
	    System.out.println("#Projected Databases = " + countLines(outputFile));
       	           
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















