package girafon.MApriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class MAprioriReducerStepK
	extends Reducer<Text,IntWritable,Text,Text> {
 

	private int support;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		support = config.getInt("support", 1);
			
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
	     sumData += Long.parseLong(splits[1]);
	     
	   }
	   String value = sumSupport + " " + sumData;
	   if (sumSupport >= support) {
		   context.write(key, new Text(value));
	   }
	}
}
