package girafon.MApriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapDuplicateCandidate  
extends Mapper<Object, Text, Text, Text>{
	
	private Integer mapID;
	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String[] parts = context.getTaskAttemptID().getTaskID().toString().split("_");
		System.out.println("-----------------MAPPER number -----------------");
		System.out.println(parts[parts.length - 1]);
		System.out.println("-------------------------------------------------------");		
		mapID = Integer.parseInt(parts[parts.length - 1]);
	}
	  
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
 
		
		context.write(new Text(mapID.toString() + " A"), value);
		 
		
		  		 
	 }
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
 
		 
	 }	 
}

