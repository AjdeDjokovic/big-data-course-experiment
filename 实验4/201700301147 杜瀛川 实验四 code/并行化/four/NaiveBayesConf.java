package four;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NaiveBayesConf {
	public int class_num;
	public ArrayList<String> classNames;
	
	public int dimen;
	public ArrayList<String> proNames;
	
	public NaiveBayesConf()
	{
		class_num = 0;
		classNames = new ArrayList<String>();
		
		dimen = 0;
		proNames = new ArrayList<String>();
	}
	
	public void ReadNaiveBayesConf(Configuration conf) throws Exception
	{
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(conf.get("conf")));
		BufferedReader d = new BufferedReader(new InputStreamReader(in));
		
		String line = d.readLine();
		String[] vals = line.split(" ");
		
		class_num = Integer.parseInt(vals[0]);
		
		for(int i = 1;i < vals.length;i++)
			classNames.add(vals[i]);
		
		line = d.readLine();
		vals = line.split(" ");
		
		dimen = Integer.parseInt(vals[0]);
		
		for(int i = 1;i < vals.length;i += 1)
		{
			proNames.add(vals[i]);
		}
		
		in.close();
		d.close();
	}

}
