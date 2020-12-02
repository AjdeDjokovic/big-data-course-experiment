package onefive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MergeSort {

	/**
	 * @param args 输入多个文件，每个文件中的每行内容均为一个整数
	 *             输出到一个新的文件中，输出的数据格式为每行两个整数，第一个数字为第二个整数的排序位次，第二个整数为原待排列的整数
	 */
	// map函数读取输入中的value，将其转化成IntWritable类型，最后作为输出key
	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

		private static IntWritable data = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String text = value.toString();
			data.set(Integer.parseInt(text));
			context.write(data, new IntWritable(1));
		}
	}

	// reduce函数将map输入的key复制到输出的value上，然后根据输入的value-list中元素的个数决定key的输出次数,定义一个全局变量line_num来代表key的位次
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static IntWritable line_num = new IntWritable(1);

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(line_num, key);
				line_num = new IntWritable(line_num.get() + 1);
			}
		}
	}

	// 自定义Partition函数，此函数根据输入数据的最大值和MapReduce框架中Partition的数量获取将输入数据按照大小分块的边界，然后根据输入数值和边界的关系返回对应的Partiton
	// ID
	public static class Partition extends Partitioner<IntWritable, IntWritable> {
		public int getPartition(IntWritable key, IntWritable value, int num_Partition) {
			int Maxnumber = 65223;// int型的最大数值
			int bound = Maxnumber / num_Partition + 1;
			int keynumber = key.get();
			for (int i = 0; i < num_Partition; i++) {
				if (keynumber < bound * (i + 1) && keynumber >= bound * i) {
					return i;
				}
			}
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "hdfs://localhost:9000/onefive/input3.txt",
				"hdfs://localhost:9000/onefive/input4.txt", "hdfs://localhost:9000/onefive/input5.txt","hdfs://localhost:9000/onefive/output" };
		if (otherArgs.length != 4) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Merge and Sort");
		job.setJarByClass(MergeSort.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]),new Path(otherArgs[1]),new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
