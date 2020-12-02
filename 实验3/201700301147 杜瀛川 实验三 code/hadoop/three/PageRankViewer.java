package three;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class PageRankViewer {

	/** 得到输入 <FromPage, <PR ToPage1,ToPage2...>> */
	/** 得到输出 <PR, FromPage> */
	public static class PageRankViewerMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] part = line.split("\t");
			double pagerank = Double.parseDouble(part[1]);

			DoubleWritable tmp = new DoubleWritable();
			tmp.set(pagerank);

			context.write(tmp, new Text(part[0]));
		}
	}

	public static class DescDoubleComparator extends DoubleWritable.Comparator {
		public double compare(WritableComparator a, WritableComparable<DoubleWritable> b) {
			return -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	/** 得到输出 <PR, FromPage> */
	/** 得到输出 <<FromPage, PR>,null> */
	public static class PageRankViewerReducer extends Reducer<DoubleWritable, Text, Text, NullWritable> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(new Text("(" + value.toString() + "," + String.format("%.10f", key.get()) + ")"), NullWritable.get());
				break;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
	      Configuration conf = new Configuration();
	      Job job3 = new Job(conf, "PageRankViewer");
	      job3.setJarByClass(PageRankViewer.class);
	      
	      job3.setMapOutputKeyClass(DoubleWritable.class);
	      job3.setMapOutputValueClass(Text.class);
	      
	      job3.setOutputKeyClass(Text.class);
	      job3.setOutputValueClass(NullWritable.class);
	      
	      job3.setSortComparatorClass(DescDoubleComparator.class);
	      job3.setMapperClass(PageRankViewerMapper.class);
	      job3.setReducerClass(PageRankViewerReducer.class);
	      
	      FileInputFormat.addInputPath(job3, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job3, new Path(args[1]));
	      job3.waitForCompletion(true);
	  }

}
