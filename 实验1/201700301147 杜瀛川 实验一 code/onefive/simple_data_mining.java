package onefive;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class simple_data_mining {
	public static int time = 0;

	/**
	 * @param args 输入一个child-parent的表格 输出一个体现grandchild-grandparent关系的表格
	 */
	// Map将输入文件按照空格分割成child和parent，然后正序输出一次作为右表，反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表区别标志
	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String child_name = new String();
			String parent_name = new String();
			String relation_type = new String();
			String line = value.toString();
			int i = 0;
			while (line.charAt(i) != ' ') {
				i++;
			}
			String[] values = { line.substring(0, i), line.substring(i + 1) };
			if (values[0].compareTo("child") != 0) {
				child_name = values[0];
				parent_name = values[1];
				relation_type = "1";// 左右表区分标志
				context.write(new Text(values[1]), new Text(relation_type + "+" + child_name + "+" + parent_name));
				// 左表
				relation_type = "2";
				context.write(new Text(values[0]), new Text(relation_type + "+" + child_name + "+" + parent_name));
				// 右表
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (time == 0) { // 输出表头
				context.write(new Text("grand_child"), new Text("grand_parent"));
				time++;
			}
			int grand_child_num = 0;
			String grand_child[] = new String[10];
			int grand_parent_num = 0;
			String grand_parent[] = new String[10];
			Iterator ite = values.iterator();
			while (ite.hasNext()) {
				String record = ite.next().toString();
				int len = record.length();
				int i = 2;
				if (len == 0)
					continue;
				char relation_type = record.charAt(0);
				String child_name = new String();
				String parent_name = new String();
				// 获取value-list中value的child

				while (record.charAt(i) != '+') {
					child_name = child_name + record.charAt(i);
					i++;
				}
				i = i + 1;
				// 获取value-list中value的parent
				while (i < len) {
					parent_name = parent_name + record.charAt(i);
					i++;
				}
				// 左表，取出child放入grand_child
				if (relation_type == '1') {
					grand_child[grand_child_num] = child_name;
					grand_child_num++;
				} else {// 右表，取出parent放入grand_parent
					grand_parent[grand_parent_num] = parent_name;
					grand_parent_num++;
				}
			}

			if (grand_parent_num != 0 && grand_child_num != 0) {
				for (int m = 0; m < grand_child_num; m++) {
					for (int n = 0; n < grand_parent_num; n++) {
						context.write(new Text(grand_child[m]), new Text(grand_parent[n]));
						// 输出结果
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "hdfs://localhost:9000/onefive/input6.txt", "hdfs://localhost:9000/onefive/output3" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Single table join");
		job.setJarByClass(simple_data_mining.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
