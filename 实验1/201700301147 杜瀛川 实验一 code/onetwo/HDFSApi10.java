package fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;

public class HDFSApi10 {
	/**
	 * 移动文件
	 */
	public static boolean mv(Configuration conf, String remoteFilePath, String remoteToFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(remoteFilePath);
		Path dstPath = new Path(remoteToFilePath);
		boolean result = fs.rename(srcPath, dstPath);
		fs.close();
		return result;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String remoteFilePath = "/user/hadoop/text.txt"; // 源文件 HDFS 路径
		String remoteToFilePath = "/user/hadoop/new.txt"; // 目的 HDFS 路径
		try {
			if (HDFSApi10.mv(conf, remoteFilePath, remoteToFilePath)) {
				System.out.println(" 将文件 " + remoteFilePath + " 移动到 " + remoteToFilePath);
			} else {
				System.out.println("操作失败(源文件不存在或移动失败)");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}