package four;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveBayesMain {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String path_conf = "hdfs://localhost:9000/iris/iris.conf";
		String path_train = "hdfs://localhost:9000/iris/iris.train";
		String path_train_result = "hdfs://localhost:9000/iris/iris.train.result";
		String path_test = "hdfs://localhost:9000/iris/iris.test";
		String path_out = "hdfs://localhost:9000/iris/iris.out";
		
		conf.set("conf", path_conf);
		conf.set("train", path_train);
		conf.set("train_result", path_train_result);
		conf.set("test", path_test);
		conf.set("out", path_out);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(path_train_result)))
			fs.delete(new Path(path_train_result), true);
		if (fs.exists(new Path(path_out)))
			fs.delete(new Path(path_out), true);
		
		Job job_train = new Job(conf, "NaiveBayesTrain");
		job_train.setJarByClass(NaiveBayesTrain.class);
		
		job_train.setMapperClass(NaiveBayesTrain.TrainMapper.class);
		job_train.setCombinerClass(NaiveBayesTrain.TrainReducer.class);
		job_train.setReducerClass(NaiveBayesTrain.TrainReducer.class);
		
		job_train.setOutputKeyClass(Text.class);
		job_train.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job_train, new Path(path_train));
		FileOutputFormat.setOutputPath(job_train, new Path(path_train_result));
		
		job_train.waitForCompletion(true);
		
		
		
		
		Job job_test = new Job(conf, "NaiveBayesTest");
		job_test.setJarByClass(NaiveBayesTest.class);
		job_test.setMapperClass(NaiveBayesTest.TestMapper.class);
		job_test.setReducerClass(NaiveBayesTest.TestReducer.class);
		
		job_test.setOutputKeyClass(IntWritable.class);
		job_test.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job_test, new Path(path_test));
		FileOutputFormat.setOutputPath(job_test, new Path(path_out));
		
		System.exit(job_test.waitForCompletion(true) ? 0 : 1);
		
		fs.close();
	}

}
