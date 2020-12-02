package four;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NaiveBayesTrainData {
	public HashMap<String,Integer> freq;
	
	public NaiveBayesTrainData()
	{
		freq = new HashMap<String,Integer>();
	}
	
	public void getData(Configuration conf) throws IOException
	{
		Path path = new Path(conf.get("train_result") + "/part-r-00000");
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);
		BufferedReader d = new BufferedReader(new InputStreamReader(in));
		
		String line;
		while ((line = d.readLine()) != null) {
			String[] res = line.split("\t");
			freq.put(res[0], new Integer(res[1]));
		}
		
		d.close();
		in.close();
	}

}
