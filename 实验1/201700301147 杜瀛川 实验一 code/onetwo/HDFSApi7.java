package fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;

public class HDFSApi7 {
	/**
	 * 判断路径是否存在
	 */
	public static boolean test(Configuration conf, String path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return fs.exists(new Path(path));
	}

	/**
	 * 判断目录是否为空 true: 空，false: 非空
	 */
	public static boolean isDirEmpty(Configuration conf, String remoteDir) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dirPath = new Path(remoteDir);
		RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(dirPath, true);
		return !remoteIterator.hasNext();
	}

	/**
	 * 创建目录
	 */
	public static boolean mkdir(Configuration conf, String remoteDir) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dirPath = new Path(remoteDir);
		boolean result = fs.mkdirs(dirPath);
		fs.close();
		return result;
	}

	/**
	 * 删除目录
	 */
	public static boolean rmDir(Configuration conf, String remoteDir) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dirPath = new Path(remoteDir);
		/* 第二个参数表示是否递归删除所有文件 */
		boolean result = fs.delete(dirPath, true);
		fs.close();
		return result;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String remoteDir = "/user/hadoop/input"; // HDFS 目录
		Boolean forceDelete = false; // 是否强制删除
		try {
			/* 判断目录是否存在，不存在则创建，存在则删除 */
			if (!HDFSApi7.test(conf, remoteDir)) {
				HDFSApi7.mkdir(conf, remoteDir); // 创建目录
				System.out.println("创建目录: " + remoteDir);
			} else {
				if (HDFSApi7.isDirEmpty(conf, remoteDir) || forceDelete) { // 目录为空或强制删除
					HDFSApi7.rmDir(conf, remoteDir);
					System.out.println("删除目录: " + remoteDir);
				} else { // 目录不为空
					System.out.println("目录不为空，不删除: " + remoteDir);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}