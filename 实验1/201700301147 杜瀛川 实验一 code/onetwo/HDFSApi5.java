package fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.text.SimpleDateFormat;

public class HDFSApi5 {
	/**
	 * 显示指定文件夹下所有文件的信息（递归）
	 */
	public static void lsDir(String remoteDir) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path dirPath = new Path(remoteDir);
		/* 递归获取目录下的所有文件 */
		RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(dirPath, true);
		/* 输出每个文件的信息 */
		while (remoteIterator.hasNext()) {
			FileStatus s = remoteIterator.next();
			System.out.println("路径: " + s.getPath().toString());
			System.out.println("权限: " + s.getPermission().toString());
			System.out.println("大小: " + s.getLen());
			/* 返回的是时间戳,转化为时间日期格式 */
			Long timeStamp = s.getModificationTime();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String date = format.format(timeStamp);
			System.out.println("时间: " + date);
			System.out.println();
		}
		fs.close();
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String remoteDir = "/user"; // HDFS 路径
		try {
			System.out.println("(递归)读取目录下所有文件的信息: " + remoteDir);
			HDFSApi5.lsDir(remoteDir);
			System.out.println("读取完成");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}