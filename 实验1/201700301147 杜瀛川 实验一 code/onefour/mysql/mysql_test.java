package mysql;

import java.sql.*;

public class mysql_test {

	/**
	 * @param args
	 */
	// JDBC DRIVER and DB
	static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	static final String DB = "jdbc:mysql://localhost/test";
	// Database auth
	static final String USER = "user1";
	static final String PASSWD = "1";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection conn = null;
		Statement stmt = null;
		try {
			// 加载驱动程序
			Class.forName(DRIVER);
			System.out.println("Connecting to a selected database...");
			// 打开一个连接
			conn = DriverManager.getConnection(DB, USER, PASSWD);
			// 执行一个查询
			stmt = conn.createStatement();
			String sql = "insert into student values('scofield',45,89,100)";
			stmt.executeUpdate(sql);
			System.out.println("Inserting records into the table successfully!");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
}
