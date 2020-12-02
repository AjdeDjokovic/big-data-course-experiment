package three;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRankIter {
	public static final double d = 0.85;

	/** 得到输入 <FromPage, <PR ToPage1,ToPage2...>> */
	/** 得到输出 <ToPage, PR> */
	/** 得到输出 <FromPage, <ToPage1,ToPage2...>> */
	public static class PageRankIterMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String part[] = line.split("\t");

			String fromPage = part[0];
			double pagerank = Double.parseDouble(part[1]);

			if (part.length > 2) {
				String[] toPageArray = part[2].split(",");
				double outPagerank = pagerank / toPageArray.length;

				for (String toPage : toPageArray)
					context.write(new Text(toPage), new Text(String.valueOf(outPagerank)));

				context.write(new Text(fromPage), new Text("|" + part[2]));
			}
		}
	}

	/** 得到输入 <ToPage, PR> */
	/** 得到输入 <FromPage, <ToPage1,ToPage2...>> */
	/** 得到输出 <FromPage, <PR ToPage1,ToPage2...>> */
	public static class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String toPageLink = "";
			double pagerank = 0;

			for (Text value : values) {
				String tmp = value.toString();

				if (tmp.charAt(0) == '|') {
					toPageLink = tmp.substring(1);
					continue;
				}

				pagerank += Double.parseDouble(tmp);
			}

			pagerank = d * pagerank + (1 - d);
			context.write(key, new Text(String.valueOf(pagerank) + "\t" + toPageLink));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job2 = new Job(conf, "PageRankIter");
		job2.setJarByClass(PageRankIter.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(PageRankIterMapper.class);
		job2.setReducerClass(PageRankIterReducer.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		job2.waitForCompletion(true);
	}
}
