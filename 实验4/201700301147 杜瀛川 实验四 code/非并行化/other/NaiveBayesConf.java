package other;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;


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
	
	public void ReadNaiveBayesConf(String confFile) throws Exception
	{
		FileInputStream in = new FileInputStream(confFile);
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
		
		d.close();
		in.close();
	}

}
