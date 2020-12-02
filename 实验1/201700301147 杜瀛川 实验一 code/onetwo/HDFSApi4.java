package fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.text.SimpleDateFormat;

public class HDFSApi4 {
	/**
	 * 显示指定文件的信息
	 */
	public static void ls(Configuration conf, String remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path remotePath = new Path(remoteFilePath);
		FileStatus[] fileStatuses = fs.listStatus(remotePath);
		for (FileStatus s : fileStatuses) {
			System.out.println("路径: " + s.getPath().toString());
			System.out.println("权限: " + s.getPermission().toString());
			System.out.println("大小: " + s.getLen());
			/* 返回的是时间戳,转化为时间日期格式 */
			Long timeStamp = s.getModificationTime();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String date = format.format(timeStamp);
			System.out.println("时间: " + date);
		}
		fs.close();
	}
	
	public static void ls1(Configuration conf, String remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path remotePath = new Path(remoteFilePath);
		FileStatus s = fs.getFileStatus(remotePath);
		System.out.println("路径: " + s.getPath().toString());
		System.out.println("权限: " + s.getPermission().toString());
		System.out.println("大小: " + s.getLen());
		/* 返回的是时间戳,转化为时间日期格式 */
		Long timeStamp = s.getModificationTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String date = format.format(timeStamp);
		System.out.println("时间: " + date);
		fs.close();
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String remoteFilePath = "/user/hadoop/text.txt"; // HDFS 路径
		try {
			System.out.println("读取文件信息: " + remoteFilePath);
			ls(conf, remoteFilePath);
			System.out.println("\n 读取完成");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}