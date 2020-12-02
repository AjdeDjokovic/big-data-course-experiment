package two;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class InvertedIndex {
	public static class FileNameInputFormat extends FileInputFormat<Text, Text> {
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileNameRecordReader fnrr = new FileNameRecordReader();
			fnrr.initialize(split, context);
			return fnrr;
		}
	}

	public static class FileNameRecordReader extends RecordReader<Text, Text> {
		String fileName;
		LineRecordReader lrr = new LineRecordReader();

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return new Text(fileName);
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return lrr.getCurrentValue();
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
			lrr.initialize(arg0, arg1);
			fileName = ((FileSplit) arg0).getPath().getName();
		}

		@Override
		public void close() throws IOException {
			lrr.close();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return lrr.nextKeyValue();
		}

		public float getProgress() throws IOException, InterruptedException {
			return lrr.getProgress();
		}

	}

	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, IntWritable> {
		private Set<String> stopwords;
		private String pattern = "[^\\w]";// 正则表达式，代表不是0-9, a-z, A-Z,-的所有其它字

		public void setup(Context context) throws IOException, InterruptedException {
			stopwords = new TreeSet<String>();
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);// 获取HDFS文件系统

			FSDataInputStream in = fs.open(new Path("hdfs://localhost:9000/two/stop_words_eng.txt"));
			BufferedReader d = new BufferedReader(new InputStreamReader(in));

			String line;
			while ((line = d.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens())
					stopwords.add(itr.nextToken());
			}
		}

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			line = line.replaceAll(pattern, " ");
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String temp = itr.nextToken();// 单词
				if (!stopwords.contains(temp)) {
					Text word = new Text();
					word.set(temp + "#" + key);
					context.write(word, new IntWritable(1));
				}
			}
		}
	}

	public static class SubCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values)
				sum += i.get();
			IntWritable res = new IntWritable();
			res.set(sum);
			context.write(key, res);
		}
	}

	public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String temp = key.toString().split("#")[0];
			return super.getPartition(new Text(temp), value, numReduceTasks);
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
		private String currentWord = new String();
		private List<String> postingList = new ArrayList<String>();

		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			String word = key.toString().split("#")[0];
			String filename = key.toString().split("#")[1];
			int sum = 0;
			for(IntWritable i:values)
				sum += i.get();
			String posting = new String("<" + filename + "," + sum + ">");//<文件名，词频>
			
			if(!word.equals(currentWord) && !currentWord.equals(""))//条件，与上一个不同,output
			{
				StringBuffer sb = new StringBuffer();
				long count = 0;
				for(String s:postingList)
				{
					sb.append(s);
					sb.append(";");
					count += Long.parseLong(s.substring(s.indexOf(',') + 1,s.indexOf('>')));
				}
				sb.append("<total," + count + ">.");
				if(count > 0)
					context.write(new Text(currentWord), new Text(sb.toString()));
				postingList = new ArrayList<String>();
			}
			currentWord = word;
			postingList.add(posting);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			StringBuffer sb = new StringBuffer();
			long count = 0;
			for(String s:postingList)
			{
				sb.append(s);
				sb.append(";");
				count += Long.parseLong(s.substring(s.indexOf(',') + 1,s.indexOf('>')));
			}
			sb.append("<total," + count + ">.");
			if(count > 0)
				context.write(new Text(currentWord), new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		args = new String[] { "hdfs://localhost:9000/two/txt_input", "hdfs://localhost:9000/two/output" };
		
		Job job = new Job(conf,"inverted index");
		job.setJarByClass(InvertedIndex.class);
		job.setInputFormatClass(FileNameInputFormat.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(SubCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setPartitionerClass(NewPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
