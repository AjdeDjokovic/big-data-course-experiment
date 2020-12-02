package four;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveBayesTest {

	public static class TestMapper extends Mapper<LongWritable,Text,IntWritable,Text>
	{
		public NaiveBayesConf nBConf;
		public NaiveBayesTrainData nBTData;
		
		/*
		 * read conf train_result
		 */
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			
			try
			{
				nBConf = new NaiveBayesConf();
				nBConf.ReadNaiveBayesConf(conf);
				nBTData = new NaiveBayesTrainData();
				nBTData.getData(conf);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		/*
		 * input	key:lineNo	value:line
		 * output	key:id		value:class_id
		 */
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			double pyi,px_yi,pxj_yi;
			double maxp = 0;
			int idx = -1;
			
			int sum,sub;
			Integer integer = nBTData.freq.get("sum");
			if(integer == null)
				sum = 0;
			else
				sum = integer.intValue();
			
			
			String[] vals = value.toString().split(" ");
			
			for(int i = 0;i < nBConf.class_num;i++)
			{
				px_yi = 1;
				String class_name = nBConf.classNames.get(i);
				integer = nBTData.freq.get(class_name);
				if(integer == null)
				{
					sub = 0;
					pyi = 0;
				}
				else
				{
					sub = integer.intValue();
					pyi = (double)sub / sum;
				}
				
				for(int j = 1;j < vals.length;j++)
				{
					String temp = class_name + "#" + (j - 1) + "#" + vals[j];
					integer = nBTData.freq.get(temp);
					if (integer == null)
						pxj_yi = 0;
					else
						pxj_yi = (double)integer.intValue() / sub;
					
					px_yi = px_yi * pxj_yi;
				}
				
				if(px_yi * pyi > maxp)
				{
					maxp = px_yi * pyi;
					idx = i;
				}
			}
			context.write(new IntWritable(Integer.parseInt(vals[0])), new Text("" + idx));
		}
	}
	
	public static class TestReducer extends Reducer<IntWritable,Text,IntWritable,Text>
	{
		public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			for(Text value:values)
			{
				context.write(key, value);
				break;
			}
		}
	}

}
