package four;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveBayesTrain {
	public static class TrainMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		private NaiveBayesConf nBConf;
		private final static IntWritable one = new IntWritable(1);
		
		/*
		 * read conf
		 */
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			nBConf = new NaiveBayesConf();
			
			try
			{
				nBConf.ReadNaiveBayesConf(conf);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			System.out.println("setup");
		}
		
		/* input	key:lineNo		value:line
		 * output	key:sum			value:1
		 * 			key:classStr	value:1
		 * 			key:classStr#deminNo#deminVal	value:1
		 */
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{		
			String[] vals = value.toString().split("\t");
			
			context.write(new Text(vals[0]),one);
			context.write(new Text("sum"), one);
			
			for(int i = 1;i < vals.length;i++)
			{
				String temp = new String();
				temp = vals[0] + "#" + (i - 1) + "#" + vals[i];
				context.write(new Text(temp), one);
			}
		}
	}
	
	 /* input	key:sum			value:1
	 * 			key:classStr	value:1
	 * 			key:classStr#deminNo#deminVal	value:1
	 * output   key:sum			value:count
	 * 			key:classStr	value:count
	 * 			key:classStr#deminNo#deminVal	value:count
	 */
	public static class TrainReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for(IntWritable val:values)
				sum += val.get();
			result.set(sum);
			context.write(key, result);
			
		}
	}
	

}
