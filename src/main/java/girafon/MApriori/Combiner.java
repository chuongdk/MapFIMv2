
package girafon.MApriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class Combiner 
	extends Reducer<Text,Text,Text,Text> {
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
	     if (splits.length > 1)
	    	 sumData += Long.parseLong(splits[1]);
	   }
	   String exportValue = sumSupport + " " + sumData;
	   if (support > 1) {
			   context.write(key, new Text(exportValue));
	   }
	   else {
			for (int i = 0; i< sumSupport; i++)
					context.write(key, new Text("1"));   
	   }
	}
}
