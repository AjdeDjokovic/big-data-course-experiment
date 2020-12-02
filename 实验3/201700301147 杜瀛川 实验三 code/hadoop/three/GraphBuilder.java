package three;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.io.Text;

public class GraphBuilder {
	/** 得到输出 <FromPage, <1.0 ToPage1,ToPage2...>> */
	public static class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] part = line.split("\t");

			String page = part[0];

			String pagerank = "1.0\t";

			context.write(new Text(page), new Text(pagerank + part[1]));
		}
	}

	public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
				break;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "Graph Builder");
		job1.setJarByClass(GraphBuilder.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(GraphBuilderMapper.class);
		job1.setReducerClass(GraphBuilderReducer.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.waitForCompletion(true);
	}
}
