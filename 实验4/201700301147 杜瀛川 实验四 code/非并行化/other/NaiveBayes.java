package other;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

public class NaiveBayes {
	private static NaiveBayesConf nBConf = new NaiveBayesConf();
	public static HashMap<String,Integer> freq = new HashMap<String,Integer>();
	
	public static void add(String key)
	{
		if(freq.get(key) == null)
			freq.put(key, new Integer(1));
		else
		{
			int temp = freq.get(key).intValue() + 1;
			freq.put(key,new Integer(temp));
		}
	}
	
	/* input	line
	 * output	key:sum			value:count
	 * 			key:classStr	value:count
	 * 			key:classStr#deminNo#deminVal	value:count
	 */
	public static void train(String trainFile) throws Exception
	{
		FileInputStream in = new FileInputStream(trainFile);
		BufferedReader d = new BufferedReader(new InputStreamReader(in));
		
		int sum = 0;
		
		String line;
		while ((line = d.readLine()) != null) {
			sum++;
			String[] vals = line.split("\t");
			add(vals[0]);
			
			for(int i = 1;i < vals.length;i++)
			{
				String temp = new String();
				temp += vals[0] + "#" + (i - 1) + "#" + vals[i];
				add(temp);
			}
		}
		freq.put("sum",new Integer(sum));
		
		d.close();
		in.close();
	}
	
	/*
	 * output	id	class_id
	 */
	public static void test(String testFile,String outFile) throws Exception
	{
		FileInputStream in = new FileInputStream(testFile);
		BufferedReader d = new BufferedReader(new InputStreamReader(in));
		
		FileOutputStream out = new FileOutputStream(outFile);
		BufferedWriter dout = new BufferedWriter(new OutputStreamWriter(out));
		
		
		
		String line;
		while ((line = d.readLine()) != null) {
			double pyi,px_yi,pxj_yi;
			double maxp = 0;
			int idx = -1;
			
			int sum,sub;
			Integer integer = freq.get("sum");
			if(integer == null)
				sum = 0;
			else
				sum = integer.intValue();
			
			
			String[] vals = line.split(" ");
			
			for(int i = 0;i < nBConf.class_num;i++)
			{
				px_yi = 1;
				String class_name = nBConf.classNames.get(i);
				integer = freq.get(class_name);
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
					integer = freq.get(temp);
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
			dout.write(vals[0] + "\t" + idx + "\n");
		}
		
		d.close();
		in.close();
		
		dout.close();
		out.close();
	}
	
	public static void main(String[] args)
	{
		String path_conf = "/home/hadoop/iris/iris.conf";
		String path_train = "/home/hadoop/iris/iris.train";
		String path_test = "/home/hadoop/iris/iris.test";
		String path_out = "/home/hadoop/iris/iris.out";
		
		try {
			nBConf.ReadNaiveBayesConf(path_conf);
			train(path_train);
			test(path_test,path_out);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("success!");
	}
}
